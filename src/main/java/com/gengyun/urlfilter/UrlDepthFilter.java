package com.gengyun.urlfilter;

import com.gengyun.entry.OnSparkInstanceFactory;
import com.gengyun.metainfo.BaseURL;
import com.gengyun.metainfo.CrawlDatum;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.CommonUtils;
import com.gengyun.utils.LogManager;
import org.apache.hadoop.io.Text;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by hadoop on 2015/11/9.
 */

public class UrlDepthFilter implements Filter, Serializable {
    private LogManager lm = new LogManager(UrlDepthFilter.class);
    private int depthCeiling; //no more than this ceiling

    public UrlDepthFilter(int depth) {
        depthCeiling = depth;
    }


    @Override
    public boolean filterDepth(Tuple2<Text, Crawldb> base) throws IOException {
        lm.logInfo("current url: " + base._1().toString() + " has a depth of " + base._2().getDepthfromSeed());

        if (base._2().getDepthfromSeed() < depthCeiling) {
            return true;
        } else {

            // CommonUtils.remove("/toCrawl");
            lm.logInfo("reach the depth ,stop crawl");
            return false;
        }
    }

    @Override
    public boolean filter(BaseURL url) throws IOException {
        lm.logDebug("current url: " + url.getUrl().toString() + " has a depth of " + url.getDepthFromSeed());

        if (url.getDepthFromSeed() < depthCeiling)
            return true;
        else {
           // OnSparkInstanceFactory.getNextURLQueueInstance().cleanQueue();

            lm.logDebug("reach the depth ,stop crawl");
            return false;
        }

    }

}
