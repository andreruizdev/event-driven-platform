package com.eventdrivenmicroservices.platform.application;

import java.util.UUID;

/** {@code scope} is {@code FULL} for a genesis-anchored scan or {@code RECENT:n} for a window of the latest n events. */
public record LedgerVerification(boolean valid, long eventsChecked, UUID firstInvalidEventId, String reason, String scope) {

    public LedgerVerification(boolean valid, long eventsChecked, UUID firstInvalidEventId, String reason) {
        this(valid, eventsChecked, firstInvalidEventId, reason, "FULL");
    }
}
