package com.eventdrivenmicroservices.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.APIRequestContext;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.options.LoadState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Renders the provisioned Grafana dashboard in a real browser against a live Kind cluster.
 * Enabled only when GRAFANA_URL and GRAFANA_ADMIN_PASSWORD are set (see the CDC drill CI job).
 */
@EnabledIfEnvironmentVariable(named = "GRAFANA_URL", matches = ".+")
class GrafanaDashboardRenderingTest {

    private static final String DASHBOARD_TITLE = "Event-Driven Microservices Lab Observability";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void provisionedDashboardRendersEveryPanelWithoutQueryErrors() throws Exception {
        String baseUrl = System.getenv("GRAFANA_URL").replaceAll("/+$", "");
        String password = System.getenv().getOrDefault("GRAFANA_ADMIN_PASSWORD", "admin");
        String authorization = "Basic " + Base64.getEncoder()
                .encodeToString(("admin:" + password).getBytes(StandardCharsets.UTF_8));
        Path outputDir = Path.of(System.getenv().getOrDefault("GRAFANA_SCREENSHOT_DIR", "build/grafana"));
        Files.createDirectories(outputDir);

        try (Playwright playwright = Playwright.create();
             Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(true))) {
            APIRequestContext api = playwright.request().newContext(new com.microsoft.playwright.APIRequest.NewContextOptions()
                    .setBaseURL(baseUrl)
                    .setExtraHTTPHeaders(java.util.Map.of("Authorization", authorization)));

            JsonNode search = objectMapper.readTree(api.get("/api/search?query=Event-Driven").text());
            String uid = null;
            for (JsonNode hit : search) {
                if (DASHBOARD_TITLE.equals(hit.path("title").asText())) {
                    uid = hit.path("uid").asText();
                }
            }
            Assertions.assertNotNull(uid, "provisioned dashboard must be discoverable through the Grafana API");

            JsonNode dashboard = objectMapper.readTree(api.get("/api/dashboards/uid/" + uid).text()).path("dashboard");
            List<String> expectedTitles = new ArrayList<>();
            for (JsonNode panel : dashboard.path("panels")) {
                expectedTitles.add(panel.path("title").asText());
            }
            Assertions.assertFalse(expectedTitles.isEmpty(), "dashboard must define panels");

            for (String name : List.of("Prometheus", "Tempo", "Loki")) {
                JsonNode health = objectMapper.readTree(
                        api.get("/api/datasources/name/" + name).text());
                String dsUid = health.path("uid").asText();
                Assertions.assertFalse(dsUid.isBlank(), "datasource " + name + " must be provisioned");
                if (!"Prometheus".equals(name)) {
                    continue;
                }
                var check = api.get("/api/datasources/uid/" + dsUid + "/health");
                Assertions.assertEquals(200, check.status(), "Prometheus datasource health: " + check.text());
            }

            List<String> queryFailures = new CopyOnWriteArrayList<>();
            List<Integer> queryResponses = new CopyOnWriteArrayList<>();
            try (BrowserContext context = browser.newContext(new Browser.NewContextOptions()
                    .setLocale("en-US").setTimezoneId("UTC").setViewportSize(1600, 4200)
                    .setExtraHTTPHeaders(java.util.Map.of("Authorization", authorization)))) {
                Page page = context.newPage();
                List<String> browserDiagnostics = new CopyOnWriteArrayList<>();
                page.onConsoleMessage(message -> {
                    if ("error".equals(message.type())) {
                        if (!message.text().contains("/api/live/ws")) {
                            browserDiagnostics.add("console: " + message.text());
                        }
                    }
                });
                page.onRequestFailed(request -> browserDiagnostics.add("failed: " + request.url() + " " + request.failure()));
                page.onResponse(response -> {
                    if (response.url().contains("/api/ds/query")) {
                        queryResponses.add(response.status());
                        String body = "";
                        try {
                            body = response.text();
                        } catch (RuntimeException ignored) {
                            // body can be unavailable if the page navigates away
                        }
                        if (response.status() != 200 || body.contains("\"error\"")) {
                            queryFailures.add(response.status() + " " + body.substring(0, Math.min(body.length(), 300)));
                        }
                    }
                });

                page.navigate(baseUrl + "/d/" + uid + "?orgId=1&from=now-30m&to=now&kiosk");
                page.waitForLoadState(LoadState.NETWORKIDLE);
                page.waitForTimeout(8000);
                page.screenshot(new Page.ScreenshotOptions()
                        .setPath(outputDir.resolve("dashboard.png")).setFullPage(true));

                var headers = page.locator("[data-testid^='data-testid Panel header']");
                List<String> renderedTitles = new ArrayList<>();
                for (int i = 0; i < headers.count(); i++) {
                    String testId = headers.nth(i).getAttribute("data-testid");
                    renderedTitles.add(testId.substring("data-testid Panel header ".length()));
                }
                Files.writeString(outputDir.resolve("browser-diagnostics.txt"), String.join("\n", browserDiagnostics));
                Files.writeString(outputDir.resolve("rendered-panels.txt"), String.join("\n", renderedTitles));
                for (String title : expectedTitles) {
                    Assertions.assertTrue(renderedTitles.contains(title),
                            "panel not rendered: " + title + " (rendered: " + renderedTitles + ")");
                }
                Assertions.assertEquals(0, page.locator("[data-testid='data-testid Panel status error']").count(),
                        "panels must not display query errors");
                int noDataPanels = page.getByText("No data", new Page.GetByTextOptions().setExact(true)).count();
                Files.writeString(outputDir.resolve("panel-summary.txt"),
                        expectedTitles.size() + " panels, " + noDataPanels + " showing No data");
                Assertions.assertTrue(noDataPanels < expectedTitles.size(),
                        "at least one panel must show live data; all " + noDataPanels + " report No data");
                Assertions.assertTrue(page.locator("canvas").count() > 0, "at least one chart canvas must render");
            }

            Assertions.assertFalse(queryResponses.isEmpty(), "Grafana must issue datasource queries");
            Assertions.assertTrue(queryFailures.isEmpty(), "datasource query failures: " + queryFailures);
        }
    }
}
