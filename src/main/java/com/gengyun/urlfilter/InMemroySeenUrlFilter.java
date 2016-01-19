package com.gengyun.urlfilter;

/**
 * Created by hadoop on 2015/11/9.
 */

import com.gengyun.metainfo.BaseURL;
import com.gengyun.metainfo.CrawlDatum;
import com.gengyun.utils.LogManager;
import org.apache.hadoop.io.Text;
import scala.Tuple2;
import tachyon.client.TachyonFS;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class InMemroySeenUrlFilter{}/* implements Filter {
    private LogManager logger = new LogManager(InMemroySeenUrlFilter.class);

    private Set<BaseURL> seen = new HashSet<BaseURL>();
    private  HashSet<String> seenUrl=new HashSet<>();

    public boolean filter(BaseURL url) {
        //logger.logDebug("current seen: " + seen.toString());
        if(seenUrl.contains(url.getUrl().toString())){
            logger.logDebug("url: " + url.getUrl().toString() + " has been seen before");
            return false;
        }else {
            seenUrl.add(url.getUrl().toString());
            logger.logDebug("url: " + url.getUrl().toString() + " has not been seen before");
            return true;
        }

        *//*if (seen.contains(url)) {
            logger.logDebug("url: " + url.getUrl() + " has been seen before");
            return false;
        } else {
            seen.add(url);
            logger.logDebug("url: " + url.getUrl() + " has not been seen before");
            return true;
        }*//*
    }

    @Override
    public boolean filterDepth(Tuple2<Text, CrawlDatum> base) throws IOException {
        return false;
    }
}*/
