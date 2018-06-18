package com.datachallenge.model;

public class UniqueUsers {
    private long timestamp; // padded to minute
    private long count;     // count of unique users

    public UniqueUsers() {

    }

    public UniqueUsers(long timestamp, long count) {
        this.timestamp = timestamp;
        this.count = count;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
