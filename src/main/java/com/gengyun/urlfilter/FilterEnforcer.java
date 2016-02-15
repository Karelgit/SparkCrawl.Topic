package com.gengyun.urlfilter;

/**
 * Created by hadoop on 2015/11/9.
 */

import com.gengyun.metainfo.BaseURL;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.LogManager;
import org.apache.hadoop.io.Text;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class FilterEnforcer implements Serializable {
    private static final long serialVersionUID = 6147898546435982641L;

    private static transient LogManager logger = new LogManager(FilterEnforcer.class);

    protected List<Filter> filters = new ArrayList<Filter>();

    //used by initializer to include filters
    public void addFilter(Filter filter) {
        filters.add(filter);
    }

    public boolean applyFilters(BaseURL baseUrl) throws IOException{
        boolean shouldCrawl = true;
        for (Filter filter : filters) {
            shouldCrawl = filter.filter(baseUrl);
            logger.logDebug("Applying filter: " + filter.getClass() + " result is: "
                    + shouldCrawl);
            if (!shouldCrawl) {
                break;
            }
        }
        return shouldCrawl;
    }


    public boolean applyDepthFilters(Tuple2<Text,Crawldb> base) throws IOException{
        boolean shouldCrawl = true;
        for (Filter filter : filters) {
            shouldCrawl = filter.filterDepth(base);
            logger.logDebug("Applying filter: " + filter.getClass() + " result is: "
                    + shouldCrawl);
            if (!shouldCrawl) {
                break;
            }
        }
        return shouldCrawl;
    }



    @Override
    public String toString() {
        return "FilterEnforcer [filters=" + filters.toString() + "]";
    }
}
