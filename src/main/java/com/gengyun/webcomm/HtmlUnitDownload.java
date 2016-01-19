package com.gengyun.webcomm;

import com.gargoylesoftware.htmlunit.*;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gengyun.entry.InstanceFactory;
import com.gengyun.metainfo.BaseWebPage;
import com.gengyun.metainfo.CrawlDatum;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.metainfo.HtmlUnitDocWebPage;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.net.UnknownHostException;

/**
 * Created by root on 15-12-16.
 */
public class HtmlUnitDownload implements Serializable {

    private static ThreadLocal<WebClient> threadWebClient = new ThreadLocal<WebClient>();
    private static String acceptLanguage;

    public HtmlUnitDownload() {
    }

    public Function<Tuple2<Text, Crawldb>, Tuple3<Text, Crawldb, HtmlPage>> download() {
        Function<Tuple2<Text, Crawldb>, Tuple3<Text, Crawldb, HtmlPage>> result = new Function<Tuple2<Text, Crawldb>, Tuple3<Text, Crawldb, HtmlPage>>() {
            @Override
            public Tuple3<Text, Crawldb, HtmlPage> call(Tuple2<Text, Crawldb> base) throws Exception {
                synchronized (Thread.currentThread()) {

                    java.util.logging.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(java.util.logging.Level.OFF);
                    java.util.logging.Logger.getLogger("org.apache.http").setLevel(java.util.logging.Level.OFF);

                    URL url = new URL(base._1().toString());

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
                        //webClient.setCache(new ExtHtmlunitCache());
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

                    Page page = null;
                    try {
                        page = webClient.getPage(req);
                    } catch (UnknownHostException e) {
                        base._2().setStatcode(800);
                        base._2().setHtml("");
                        return new Tuple3<>(base._1(), base._2(), (HtmlPage) page);
                    } catch (org.apache.http.conn.ConnectTimeoutException e) {
                        base._2().setStatcode(801);
                        base._2().setHtml("");
                        return new Tuple3<>(base._1(), base._2(), (HtmlPage) page);
                    } catch (java.net.SocketTimeoutException e) {
                        base._2().setStatcode(802);
                        base._2().setHtml("");
                        return new Tuple3<>(base._1(), base._2(), (HtmlPage) page);
                    }


                    if (page.isHtmlPage() && StringUtils.equals(page.getWebResponse().getContentType(), "text/html")) {
                        BaseWebPage result = InstanceFactory.getOneHtmlunitWebPage();
                        ((HtmlUnitDocWebPage) result).setDoc((HtmlPage) page);
                        base._2().setStatcode(page.getWebResponse().getStatusCode());
                        base._2().setHtml(((HtmlPage) page).asXml().replaceFirst("<\\?xml version=\"1.0\" encoding=\"(.+)\"\\?>", "<!DOCTYPE html>"));
                        base._2().setCrawltime(System.currentTimeMillis());
                        return new Tuple3<>(base._1(), base._2(), (HtmlPage) page);
                    } else {
                        return new Tuple3<>(base._1(), base._2(), null);
                    }
                }

            }
        };
        return result;
    }
}
