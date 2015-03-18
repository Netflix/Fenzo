package com.netflix.fenzo.triggers;

/**
 *
 * Date: 1/12/15
 * Time: 2:31 PM
 */
public enum Status {
    CANCELLED("CANCELLED", true), COMPLETED("COMPLETED", true), FAILED("FAILED", true), SKIPPED("SKIPPED", true),
    SCHEDULED("SCHEDULED", false), IN_PROGRESS("IN_PROGRESS", false), DISABLED("DISABLED", false),
    ENABLED("ENABLED", false);

    private final String status;
    private final boolean complete;
    private Throwable throwable;

    Status(String status, boolean complete) {
        this.status = status;
        this.complete = complete;
        this.throwable = null;
    }

    public String getStatus() {
        return status;
    }

    public boolean isComplete() {
        return complete;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }
}
