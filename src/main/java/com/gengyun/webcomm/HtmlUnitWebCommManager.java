package com.gengyun.webcomm;

import com.gargoylesoftware.htmlunit.*;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gengyun.entry.InstanceFactory;
import com.gengyun.metainfo.BaseURL;
import com.gengyun.metainfo.BaseWebPage;
import com.gengyun.metainfo.CrawlDatum;
import com.gengyun.metainfo.HtmlUnitDocWebPage;
import com.gengyun.utils.LogManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URL;


/**
 * htmlunit建立web连接
 * Created by hadoop on 2015/11/17.
 */
public class HtmlUnitWebCommManager implements WebCommManager {
    private LogManager logger = new LogManager(HtmlUnitWebCommManager.class);

    private static ThreadLocal<WebClient> threadWebClient = new ThreadLocal<WebClient>();
    private static String acceptLanguage;

    @Override
    public BaseWebPage fetchWebPage(CrawlDatum base) {
        synchronized (Thread.currentThread()) {
            try {
                java.util.logging.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(java.util.logging.Level.OFF);
                java.util.logging.Logger.getLogger("org.apache.http").setLevel(java.util.logging.Level.OFF);

                URL url = new URL(base.getUrl().toString());

                WebRequest req = new WebRequest(url);
                req.setAdditionalHeader("Accept-Language", acceptLanguage);
                //req.setAdditionalHeader("Cookie", "");

                WebClient webClient = threadWebClient.get();
                if (webClient == null) {

                    webClient = new WebClient();
                    webClient.getOptions().setJavaScriptEnabled(true);
                    webClient.getOptions().setCssEnabled(false);
                    webClient.getOptions().setAppletEnabled(false);
                    webClient.getCookieManager().setCookiesEnabled(true);
                    webClient.getOptions().setRedirectEnabled(true);
                    webClient.getOptions().setThrowExceptionOnScriptError(false);
                    webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
                    webClient.getOptions().setTimeout(15 * 1000);
                    // AJAX support
                    webClient.setAjaxController(new NicelyResynchronizingAjaxController());
                    // Use extension version htmlunit cache process
                    webClient.setCache(new ExtHtmlunitCache());
                    // Enhanced WebConnection based on urlfilter
                    //webClient.setWebConnection(new RegexHttpWebConnection(webClient));
                    webClient.waitForBackgroundJavaScript(5L * 1000);
                    webClient.setJavaScriptTimeout(0);
                    //设置足够高度以支持一些需要页面内容多需屏幕滚动显示的页面
                    webClient.getCurrentWindow().setInnerHeight(Integer.MAX_VALUE);
                    if (acceptLanguage == null) {
                        acceptLanguage = "zh-cn,zh;en-us,en-gb,en;q=0.7,*;q=0.3";
                    }
                    threadWebClient.set(webClient);
                }
                Page page = webClient.getPage(req);
                if (page.isHtmlPage() && StringUtils.equals(page.getWebResponse().getContentType(), "text/html")) {
                    BaseWebPage result = InstanceFactory.getOneHtmlunitWebPage();
                    ((HtmlUnitDocWebPage) result).setDoc((HtmlPage) page);
                    base.setWebPage(result);
                    base.setStatcode(page.getWebResponse().getStatusCode());
                    base.setText(((HtmlPage) page).asText());
                    base.setHtml(((HtmlPage) page).asXml().replaceFirst("<\\?xml version=\"1.0\" encoding=\"(.+)\"\\?>", "<!DOCTYPE html>"));
                    base.setCrawltime(System.currentTimeMillis());
                    base.setFetched(true);
                    return result;
                } else {
                    return null;
                }
            } catch (IOException e) {
                logger.logError(e.toString());
            }
            return null;
        }
    }

  /*  @Override
    public BaseWebPage fetchPage(BaseURL url) {
        synchronized (Thread.currentThread()) {
            try {
                java.util.logging.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(java.util.logging.Level.OFF);
                java.util.logging.Logger.getLogger("org.apache.http").setLevel(java.util.logging.Level.OFF);
                WebRequest req = new WebRequest(url.getUrl());
                req.setAdditionalHeader("Accept-Language", acceptLanguage);
                //req.setAdditionalHeader("Cookie", "");

                WebClient webClient = threadWebClient.get();
                if (webClient == null) {

                    webClient = new WebClient();
                    webClient.getOptions().setJavaScriptEnabled(true);
                    webClient.getOptions().setCssEnabled(false);
                    webClient.getOptions().setAppletEnabled(false);
                    webClient.getCookieManager().setCookiesEnabled(true);
                    webClient.getOptions().setRedirectEnabled(true);
                    webClient.getOptions().setThrowExceptionOnScriptError(false);
                    webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
                    webClient.getOptions().setTimeout(15 * 1000);
                    // AJAX support
                    webClient.setAjaxController(new NicelyResynchronizingAjaxController());
                    // Use extension version htmlunit cache process
                    webClient.setCache(new ExtHtmlunitCache());
                    // Enhanced WebConnection based on urlfilter
                    webClient.setWebConnection(new RegexHttpWebConnection(webClient));
                    webClient.waitForBackgroundJavaScript(5L * 1000);
                    // webClient.setJavaScriptTimeout(0);
                    //设置足够高度以支持一些需要页面内容多需屏幕滚动显示的页面
                    webClient.getCurrentWindow().setInnerHeight(Integer.MAX_VALUE);
                    if (acceptLanguage == null) {
                        acceptLanguage = "zh-cn,zh;en-us,en-gb,en;q=0.7,*;q=0.3";
                    }
                    threadWebClient.set(webClient);
                }
                Page page = webClient.getPage(url.getUrl());

                if (page.isHtmlPage() && StringUtils.equals(page.getWebResponse().getContentType(), "text/html")) {
                    BaseWebPage result = InstanceFactory.getOneHtmlunitWebPage();
                    ((HtmlUnitDocWebPage) result).setDoc((HtmlPage) page);
                    return result;
                } else {
                    return null;
                }
            } catch (Exception e) {
                logger.logError(e.toString());
            }
            return null;
        }

    }*/


}
