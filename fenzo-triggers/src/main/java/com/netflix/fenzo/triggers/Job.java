package com.netflix.fenzo.triggers;

import java.util.List;
import java.util.Map;

/**
 * Any class that implements the {@code Job} interface can be executed as a {@code Trigger}
 */
public interface Job {

    /**
     * A job can override the current event status and return whatever status it wants to
     * @return
     */
    public Status getStatus();

    /**
     * A job can override the trigger owners.
     * For example, for a given deployment job, the list of committers could be the owners of the job
     * @return
     */
    public List<String> getOwners();

    /**
     * A job can override the trigger watchers
     * For example, for a given deployment job, the list of designated on-calls could be the watchers of the job
     * @return
     */
    public List<String> getWatchers();

    /**
     * Executes the job
     * @param params
     * @throws Exception
     */
    public void execute(Map<String,Object> params) throws Exception;

    /**
     * A job can override the cancel behavior for itself
     * @throws Exception
     */
    public void cancel() throws Exception;

    /**
     * A job can return execution log
     * @return
     */
    public List<String> getExecutionLog();

    /**
     * Gets called before an event for a trigger starts starts
     * @param owners
     * @param watchers
     */
    public void notifyForEventStart(List<String> owners, List<String> watchers);

    /**
     * Gets called after an event for a trigger starts ends
     * @param owners
     * @param watchers
     */
    public void notifyForEventEnd(List<String> owners, List<String> watchers);

    /**
     * Gets called if an error occurred while executing the job
     * @param owners
     * @param watchers
     */
    public void notifyForError(List<String> owners, List<String> watchers, Throwable throwable);

    /**
     * Gets called if an event gets cancelled
     * @param owners
     * @param watchers
     */
    public void notifyForEventCancel(List<String> owners, List<String> watchers);

}
