package com.gengyun.crawler;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by hadoop on 2015/11/11.
 */
public class MyUrl implements Serializable {
    private String url;
    private String parentUrl;
    private String time;
    private String depth;
    private String fetched;

    public String getDepth() {
        return depth;
    }

    public void setDepth(String depth) {
        this.depth = depth;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getParentUrl() {
        return parentUrl;
    }

    public void setParentUrl(String parentUrl) {
        this.parentUrl = parentUrl;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getFetched() {
        return fetched;
    }

    public void setFetched(String fetched) {
        this.fetched = fetched;
    }
}
