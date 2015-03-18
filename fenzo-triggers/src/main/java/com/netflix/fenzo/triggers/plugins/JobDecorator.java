package com.netflix.fenzo.triggers.plugins;

import com.netflix.fenzo.triggers.Job;

import java.util.Map;

/**
 * Users can register a {@code JobDecorator} with the {@code Trigger} to decorate
 * their job instances
 *
 */
public interface JobDecorator {
    public Job decorate(Job job, Map<String, Object> params);
}
