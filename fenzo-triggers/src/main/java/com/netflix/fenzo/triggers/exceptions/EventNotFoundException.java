package com.netflix.fenzo.triggers.exceptions;

/**
 *
 */
public class EventNotFoundException extends Exception {

    public EventNotFoundException(String message) {
        super(message);
    }

    public EventNotFoundException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
