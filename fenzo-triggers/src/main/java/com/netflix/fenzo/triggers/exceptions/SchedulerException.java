package com.netflix.fenzo.triggers.exceptions;

/**
 *
 */
public class SchedulerException extends Exception {

    public SchedulerException(String message) {
        super(message);
    }

    public SchedulerException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
