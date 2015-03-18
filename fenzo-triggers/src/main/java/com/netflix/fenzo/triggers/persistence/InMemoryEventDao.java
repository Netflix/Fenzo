package com.netflix.fenzo.triggers.persistence;

import com.netflix.fenzo.triggers.Event;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class InMemoryEventDao implements EventDao {

    private final ConcurrentMap<String, Map<String,Event>> triggerEvents = new ConcurrentHashMap<String, Map<String,Event>>();
    private final ConcurrentMap<String, Event> allEvents = new ConcurrentHashMap<String, Event>();

    @Override
    public void createEvent(String triggerId, Event event) {
        Map eventMap = new HashMap();
        eventMap.put(event.getId(), event);
        Map existingEventMap = triggerEvents.putIfAbsent(triggerId, eventMap);
        if (existingEventMap != null) {
            synchronized (triggerEvents) {
                triggerEvents.get(triggerId).put(event.getId(), event);
            }
        }
        allEvents.put(event.getId(), event);
    }

    @Override
    public void updateEvent(String triggerId, Event event) {
        createEvent(triggerId, event);
    }

    @Override
    public Event getEvent(String eventId) {
        return allEvents.get(eventId);
    }

    @Override
    public void deleteEvent(String triggerId, Event event) {
        triggerEvents.get(triggerId).remove(event.getId());
        allEvents.remove(event.getId());
    }

    @Override
    public List<Event> getEvents(String triggerId, int count) {
        List<Event> eventsList = getEvents(triggerId);
        return eventsList.size() > count ? eventsList.subList(0, count) : eventsList;
    }

    @Override
    public List<Event> getEvents(String triggerId) {
        return (List<Event>) triggerEvents.get(triggerId).values();
    }
}
