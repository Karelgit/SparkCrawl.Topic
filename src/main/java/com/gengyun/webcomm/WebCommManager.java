package com.gengyun.webcomm;

import com.gengyun.metainfo.BaseWebPage;
import com.gengyun.metainfo.CrawlDatum;

/**
 * Created by hadoop on 2015/11/9.
 */
public interface WebCommManager {
    //public BaseWebPage fetchPage(BaseURL url);

    public  BaseWebPage fetchWebPage(CrawlDatum base);
}
