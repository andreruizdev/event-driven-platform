package com.eventdrivenmicroservices.platform.api;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import reactor.core.publisher.Mono;

@Controller
public class IndexController {

    private final Resource indexHtml = new ClassPathResource("static/index.html");

    @GetMapping(value = "/", produces = MediaType.TEXT_HTML_VALUE)
    @ResponseBody
    public Mono<Resource> index() {
        return Mono.just(indexHtml);
    }
}
