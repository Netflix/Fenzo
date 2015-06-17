package com.netflix.fenzo.triggers.exceptions;

/**
 *
 */
public class TriggerNotFoundException extends Exception {

    public TriggerNotFoundException(String message) {
        super(message);
    }

    public TriggerNotFoundException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
