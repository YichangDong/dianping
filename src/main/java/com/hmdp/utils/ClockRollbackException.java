package com.hmdp.utils;

public class ClockRollbackException extends RuntimeException {

    private final long offsetMs;

    public ClockRollbackException(long offsetMs) {
        super("Clock moved backwards by " + offsetMs + "ms");
        this.offsetMs = offsetMs;
    }

    public long getOffsetMs() {
        return offsetMs;
    }
}
