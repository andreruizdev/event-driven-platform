package com.eventdrivenmicroservices.platform.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import reactor.core.publisher.Mono;

import java.net.URI;

@Controller
public class IndexController {

    @GetMapping("/")
    public Mono<Void> index(ServerHttpResponse response) {
        response.setStatusCode(HttpStatus.FOUND);
        response.getHeaders().setLocation(URI.create("/index.html"));
        return response.setComplete();
    }
}
