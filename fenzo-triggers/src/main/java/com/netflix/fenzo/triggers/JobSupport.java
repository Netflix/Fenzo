package com.netflix.fenzo.triggers;

import java.util.List;

/**
 *
 */
public abstract class JobSupport implements Job {

    @Override
    public Status getStatus() {
        return null;
    }

    @Override
    public List<String> getOwners() {
        return null;
    }

    @Override
    public List<String> getWatchers() {
        return null;
    }

    @Override
    public List<String> getExecutionLog() {
        return null;
    }

    @Override
    public void notifyForEventStart(List<String> owners, List<String> watchers) {
        // Do nothing. If job wants to notify then override this method
    }

    @Override
    public void notifyForEventEnd(List<String> owners, List<String> watchers) {
        // Do nothing. If job wants to notify then override this method
    }

    @Override
    public void notifyForEventCancel(List<String> owners, List<String> watchers) {
        // Do nothing. If job wants to notify then override this method
    }

    @Override
    public void notifyForError(List<String> owners, List<String> watchers, Throwable t) {
        // Do nothing. If job wants to notify then override this method
    }

}
