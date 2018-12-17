package com.openspark.sqlstream.base;

public class WindowType {

    private String type;

    private String window;

    public WindowType(String type, String window) {
        this.type = type;
        this.window = window;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getWindow() {
        return window;
    }

    public void setWindow(String window) {
        this.window = window;
    }
}
