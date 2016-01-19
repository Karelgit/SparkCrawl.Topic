package com.gengyun.queue;

/**
 * Created by hadoop on 2015/11/9.
 */

import com.gengyun.metainfo.BaseURL;
import com.gengyun.utils.LogManager;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;


public class InMemorySingleProcessBasicQueue implements URLQueue {
    private LogManager logger = new LogManager(InMemorySingleProcessBasicQueue.class);

    private Queue<BaseURL> nextUrls = new LinkedBlockingDeque<BaseURL>();


    public boolean hasMoreUrls() {
        logger.logInfo("current queue: " + nextUrls.toString());
        return !this.nextUrls.isEmpty();
    }


    public BaseURL getNextUrl() {
        return this.nextUrls.poll();
    }

    public void putNextUrls(List<BaseURL> nextUrls) {
        if (nextUrls.size() != 0 && this.nextUrls.size() < 10000) {
            this.nextUrls.addAll(nextUrls);
        }
    }

}
