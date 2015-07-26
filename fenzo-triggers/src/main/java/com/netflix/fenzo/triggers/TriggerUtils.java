package com.netflix.fenzo.triggers;

import org.quartz.CronExpression;

import java.text.ParseException;
import java.util.StringTokenizer;

/**
 * @author sthadeshwar
 */
public class TriggerUtils {

    public static boolean isValidCronExpression(String cronExpression) {
        try {
            validateCronExpression(cronExpression);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void validateCronExpression(String cronExpression) throws Exception {
        try {
            if (cronExpression == null || cronExpression.equals("")) {
                throw new Exception(String.format("Cron expression cannot be null or empty : %s", cronExpression));
            }
            StringTokenizer tokenizer = new StringTokenizer(cronExpression, " \t", false);
            int tokens = tokenizer.countTokens();
            String beginningToken = tokenizer.nextToken().trim();
            if ("*".equals(beginningToken)) {
                // For all practical purposes and for ALL clients of this library, this is true!
                throw new Exception(String.format("Cron expression cannot have '*' in the SECONDS (first) position : %s",
                    cronExpression));
            }
            if (tokens > 7) {
                throw new Exception(String.format("Cron expression cannot have more than 7 fields : %s", cronExpression));
            }
            CronExpression.validateExpression(cronExpression);
        } catch (ParseException e) {
            throw new Exception(e.getMessage());
        }
    }
}
