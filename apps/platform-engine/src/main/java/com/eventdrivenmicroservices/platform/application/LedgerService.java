package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.domain.LedgerEvent;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LedgerEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class LedgerService {

    private final LedgerEventRepository ledgerEventRepository;
    private static final String GENESIS_HASH = "0000000000000000000000000000000000000000000000000000000000000000";

    public Mono<LedgerEvent> appendToLedger(String transactionType, String payload) {
        return ledgerEventRepository.findLatestHash()
                .defaultIfEmpty(GENESIS_HASH)
                .flatMap(previousHash -> {
                    String dataToHash = previousHash + transactionType + payload;
                    String currentHash = calculateSHA256(dataToHash);
                    
                    LedgerEvent event = LedgerEvent.builder()
                            .transactionType(transactionType)
                            .payload(payload)
                            .previousHash(previousHash)
                            .currentHash(currentHash)
                            .build();
                            
                    return ledgerEventRepository.save(event);
                })
                .doOnSuccess(event -> log.info("Appended to ledger. Hash: {}", event.getCurrentHash()))
                .doOnError(e -> log.error("Failed to append to ledger", e));
    }

    public Mono<LedgerEvent> appendEvent(String transactionType, String payload) {
        return appendToLedger(transactionType, payload);
    }

    private String calculateSHA256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedhash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(encodedhash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
    }

    private String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
