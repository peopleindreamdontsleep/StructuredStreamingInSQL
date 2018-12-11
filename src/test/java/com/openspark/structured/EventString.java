package com.openspark.structured;

import java.sql.Timestamp;

public class EventString {

    private Timestamp timestamp;

    private String word;

    public EventString() {
    }

    public EventString(Timestamp timestamp, String word) {
        this.timestamp = timestamp;
        this.word = word;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public static void main(String[] args) {
        String s = "aa|bb";
        System.out.println(s.split("\\|")[0]);
    }
}
