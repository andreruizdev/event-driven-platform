package com.eventdrivenmicroservices.platform.config;

/** Neutralises caller-supplied values before they are written to log lines (CWE-117). */
public final class LogSanitizer {

    private static final int MAX_LENGTH = 128;

    private LogSanitizer() {
    }

    public static String safe(Object value) {
        if (value == null) {
            return "null";
        }
        String text = value.toString();
        StringBuilder out = new StringBuilder(Math.min(text.length(), MAX_LENGTH));
        for (int i = 0; i < text.length() && out.length() < MAX_LENGTH; i++) {
            char c = text.charAt(i);
            out.append(Character.isISOControl(c) ? '_' : c);
        }
        if (text.length() > MAX_LENGTH) {
            out.append("...");
        }
        return out.toString();
    }
}
