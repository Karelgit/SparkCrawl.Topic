package com.gengyun.urlidentifier;

/**
 * Created by hadoop on 2015/11/9.
 */

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.gengyun.metainfo.BaseURL;
import com.gengyun.metainfo.CrawlDatum;
import com.gengyun.metainfo.JsoupDocWebPage;
import com.gengyun.utils.LogManager;
import org.apache.hadoop.io.Text;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import scala.Tuple2;

public class JsoupBasedURLIdentifier{} /*implements URLIdentifier {
    private LogManager logger = new LogManager(JsoupBasedURLIdentifier.class);

    public List<BaseURL> extractUrls(BaseURL url) {
        return grabHTMLLinks(url);
    }

    @Override
    public List<Tuple2<Text, CrawlDatum>> extractCrawlDatumUrls(Tuple2<Text, CrawlDatum> base) {
        return null;
    }


    private List<BaseURL> grabHTMLLinks(BaseURL url) {
        List<BaseURL> urlsToCrawl = new ArrayList<BaseURL>();
        if (!(url.getPageContent() instanceof JsoupDocWebPage)) {
            logger.logError("JsoupBasedURLIdentifier must be used together with JsoupDocWebPage");
            return urlsToCrawl;
        }

        Document doc = ((JsoupDocWebPage) url.getPageContent()).getDoc();
        for (Element ahref : doc.select("a[href]")) {
            String absUrl = ahref.attr("abs:href");
            String title = "";
            if (ahref.hasAttr("title")) {
                title = ahref.attr("title");
            } else if (ahref.hasText()) {
                title = ahref.text();
            }


            logger.logInfo("discover url on page: " + absUrl + "\t" + title+"\t"+url.getDepthFromSeed());
            putUrlToCrawl(absUrl, title, url, urlsToCrawl);
        }
        for (Element linkhref : doc.select("link[href]")) {
            String absUrl = linkhref.attr("abs:href");
            String title = "";
            if (linkhref.hasAttr("title")) {
                title = linkhref.attr("title");
            } else if (linkhref.hasText()) {
                title = linkhref.text();
            }

            logger.logInfo("discover link on page: " + absUrl + "\t" + title+"\t"+url.getDepthFromSeed());
            putUrlToCrawl(absUrl, title, url, urlsToCrawl);
        }
        return urlsToCrawl;
    }

    private void putUrlToCrawl(String absUrl, String absUrlTitle, BaseURL url, List<BaseURL> urlsToCrawl) {
        int questionMarkLocation = absUrl.indexOf("?");
        if (questionMarkLocation > -1) {
            logger.logDebug("link before chunking: " + absUrl);
            absUrl = absUrl.substring(0, questionMarkLocation);
            logger.logDebug("link after chunking: " + absUrl);
        }
        BaseURL obj = null;
        try {
            obj = new BaseURL(new URL(absUrl));
            obj.setTitle(absUrlTitle);
            obj.setDepthFromSeed(url.getDepthFromSeed() + 1);
            urlsToCrawl.add(obj);
        } catch (MalformedURLException e) {
            logger.logError("exception during creating url: " + e.toString());
        }
    }
}*/
