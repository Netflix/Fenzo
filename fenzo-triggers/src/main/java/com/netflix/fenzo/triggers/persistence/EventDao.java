package com.netflix.fenzo.triggers.persistence;

import com.netflix.fenzo.triggers.Event;

import java.util.List;

/**
 *
 */
public interface EventDao {

    public void createEvent(String triggerId, Event event);
    public void updateEvent(String triggerId, Event event);
    public Event getEvent(String eventId);
    public void deleteEvent(String triggerId, Event event);
    public List<Event> getEvents(String triggerId, int count);
    public List<Event> getEvents(String triggerId);

}
