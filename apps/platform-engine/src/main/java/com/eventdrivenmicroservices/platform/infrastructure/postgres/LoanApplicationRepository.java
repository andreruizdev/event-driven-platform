package com.eventdrivenmicroservices.platform.infrastructure.postgres;

import com.eventdrivenmicroservices.platform.model.LoanApplication;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import java.util.UUID;

public interface LoanApplicationRepository extends ReactiveCrudRepository<LoanApplication, UUID> {}
