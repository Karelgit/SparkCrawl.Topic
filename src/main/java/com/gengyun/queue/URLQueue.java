package com.gengyun.queue;

import com.gengyun.metainfo.BaseURL;

import java.util.List;

/**
 * Created by hadoop on 2015/11/9.
 */
public interface URLQueue {
    public boolean hasMoreUrls();

    public BaseURL getNextUrl();

    public void putNextUrls(List<BaseURL> nextUrls);
}
