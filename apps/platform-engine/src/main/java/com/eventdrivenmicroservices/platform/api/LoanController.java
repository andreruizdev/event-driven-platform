package com.eventdrivenmicroservices.platform.api;

import com.eventdrivenmicroservices.platform.model.LoanApplication;
import com.eventdrivenmicroservices.platform.model.LoanApplicationRequest;
import com.eventdrivenmicroservices.platform.application.LoanService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/loans")
@RequiredArgsConstructor
@Validated
public class LoanController {
    private final LoanService loanService;

    @PostMapping
    public Mono<ResponseEntity<LoanApplication>> submit(@Valid @RequestBody LoanApplicationRequest request) {
        return loanService.submitApplication(request)
                .map(ResponseEntity::ok);
    }
}
