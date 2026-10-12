package com.eventdrivenmicroservices.platform.application;

import com.eventdrivenmicroservices.platform.domain.LedgerEvent;
import com.eventdrivenmicroservices.platform.infrastructure.postgres.LedgerEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
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
    private final DatabaseClient databaseClient;
    private final TransactionalOperator transactionalOperator;
    private static final String GENESIS_HASH = "0000000000000000000000000000000000000000000000000000000000000000";
    // Held until the surrounding transaction commits, serializing head-read and insert across replicas.
    private static final String APPEND_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext('ledger_events'))";

    public Mono<LedgerEvent> appendToLedger(String transactionType, String payload) {
        return appendToLedger(transactionType, payload, null, null);
    }

    public Mono<LedgerEvent> appendToLedger(
            String transactionType, String payload, UUID outboxEventId, String correlationId) {
        return databaseClient.sql(APPEND_LOCK_SQL).fetch().all().then(ledgerEventRepository.findLatestHash())
                .defaultIfEmpty(GENESIS_HASH)
                .flatMap(previousHash -> {
                    String dataToHash = previousHash + transactionType + payload;
                    String currentHash = calculateSHA256(dataToHash);
                    
                    LedgerEvent event = LedgerEvent.builder()
                            .transactionType(transactionType)
                            .payload(payload)
                            .outboxEventId(outboxEventId)
                            .correlationId(correlationId)
                            .previousHash(previousHash)
                            .currentHash(currentHash)
                            .build();
                            
                    return ledgerEventRepository.save(event);
                })
                .as(transactionalOperator::transactional)
                .doOnSuccess(event -> log.info("Appended to ledger. Hash: {}", event.getCurrentHash()))
                .doOnError(e -> log.error("Failed to append to ledger", e));
    }

    public Mono<LedgerVerification> verifyChain() {
        return ledgerEventRepository.findAllInChainOrder()
                .reduce(new ChainState(GENESIS_HASH, 0, null, null), this::advance)
                .map(state -> new LedgerVerification(
                        state.invalidEventId() == null, state.checked(), state.invalidEventId(), state.reason(), "FULL"));
    }

    /**
     * Verifies only the latest {@code recent} events. The first event's stored previous hash is trusted as the
     * anchor, so tampering before the window is not detected; use {@link #verifyChain()} for that.
     */
    public Mono<LedgerVerification> verifyRecent(long recent) {
        return ledgerEventRepository.findRecentInChainOrder(recent)
                .reduce(new ChainState(null, 0, null, null), this::advance)
                .map(state -> new LedgerVerification(
                        state.invalidEventId() == null, state.checked(), state.invalidEventId(), state.reason(), "RECENT:" + recent));
    }

    private ChainState advance(ChainState state, LedgerEvent event) {
        long checked = state.checked() + 1;
        if (state.invalidEventId() != null) {
            return new ChainState(state.expectedPreviousHash(), checked, state.invalidEventId(), state.reason());
        }
        if (state.expectedPreviousHash() != null && !state.expectedPreviousHash().equals(event.getPreviousHash())) {
            return new ChainState(state.expectedPreviousHash(), checked, event.getId(), "previous hash does not link to prior event");
        }
        String recomputed = calculateSHA256(event.getPreviousHash() + event.getTransactionType() + event.getPayload());
        if (!recomputed.equals(event.getCurrentHash())) {
            return new ChainState(state.expectedPreviousHash(), checked, event.getId(), "current hash does not match event content");
        }
        return new ChainState(event.getCurrentHash(), checked, null, null);
    }

    private record ChainState(String expectedPreviousHash, long checked, UUID invalidEventId, String reason) {
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
