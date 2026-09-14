$ErrorActionPreference = "Stop"
Write-Host "Running Infrastructure Security Scanner..."

$helmPath = "deploy/helm/event-driven-lab/templates"
$files = Get-ChildItem -Path $helmPath -Filter "*.yaml"

foreach ($file in $files) {
    $content = Get-Content $file.FullName
    if ($content -match "(?m)^kind:\s*(Deployment|StatefulSet)") {
        if (-not ($content -match "securityContext:")) {
            Write-Host "=========================================================================="
            Write-Host "❌ GOVERNANCE FAILURE: Infrastructure Security Violation!"
            Write-Host "File '$($file.Name)' does not define a 'securityContext'."
            Write-Host "Rule #5 (AGENTS.md): Default to the most restrictive settings possible."
            Write-Host "=========================================================================="
            exit 1
        }
        if (-not ($content -match "runAsNonRoot" -or $content -match "allowPrivilegeEscalation" -or $content -match "readOnlyRootFilesystem" -or $content -match "capabilities")) {
            Write-Host "=========================================================================="
            Write-Host "⚠️  GOVERNANCE WARNING: Incomplete securityContext constraints in '$($file.Name)'."
            Write-Host "Ensure containers explicitly specify runAsNonRoot, allowPrivilegeEscalation, or capabilities."
            Write-Host "=========================================================================="
        }
    }
}
Write-Host "✅ All Helm charts passed security validation."
exit 0
