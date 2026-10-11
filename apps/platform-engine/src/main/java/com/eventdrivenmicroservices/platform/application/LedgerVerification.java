package com.eventdrivenmicroservices.platform.application;

import java.util.UUID;

public record LedgerVerification(boolean valid, long eventsChecked, UUID firstInvalidEventId, String reason) {
}
