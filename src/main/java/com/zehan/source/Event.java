package com.zehan.source;


public class Event {
    public String user;
    public String url;
    public long timestamp;

    public Event() {
    }

    public Event(String user, String url, long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

}
