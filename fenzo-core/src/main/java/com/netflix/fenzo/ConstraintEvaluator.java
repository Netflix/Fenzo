package com.netflix.fenzo;

public interface ConstraintEvaluator {
    public static class Result {
        private final boolean isSuccessful;
        private final String failureReason;

        public Result(boolean successful, String failureReason) {
            isSuccessful = successful;
            this.failureReason = isSuccessful? "" : failureReason;
        }
        public boolean isSuccessful() {
            return isSuccessful;
        }
        public String getFailureReason() {
            return failureReason;
        }
    }
    public String getName();
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM,
                           TaskTrackerState taskTrackerState);
}
