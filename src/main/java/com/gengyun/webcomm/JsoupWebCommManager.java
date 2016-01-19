package com.gengyun.webcomm;

import java.io.IOException;

import com.gengyun.entry.InstanceFactory;
import com.gengyun.metainfo.BaseURL;
import com.gengyun.metainfo.BaseWebPage;
import com.gengyun.metainfo.CrawlDatum;
import com.gengyun.metainfo.JsoupDocWebPage;
import com.gengyun.utils.LogManager;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * 建立jsoup连接
 * Created by hadoop on 2015/11/9.
 */
public class JsoupWebCommManager implements WebCommManager {
    private LogManager logger = new LogManager(JsoupWebCommManager.class);

    public BaseWebPage fetchPage(BaseURL url) {

        try {
            Document doc = Jsoup.connect(url.getUrl().toString()).timeout(60 * 1000).userAgent("Mozilla").get();
            //logger.logDebug("doc: " + doc.toString());
            BaseWebPage result = InstanceFactory.getOneWebPage();
            ((JsoupDocWebPage) result).setDoc(doc);
            return result;
        } catch (IOException e) {
            logger.logError(e.toString());
        }
        return null;
    }

    @Override
    public BaseWebPage fetchWebPage(CrawlDatum base) {
        return null;
    }
}
