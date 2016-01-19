package com.gengyun.lsc.analysis.tool;

import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.Page;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gengyun.webcomm.ExtHtmlunitCache;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;

/**
 * Created by root on 15-12-16.
 */
public class GetHtml implements Serializable
{

    private static String acceptLanguage;

    public static  String getHtmlFromUrl(String str) throws IOException
    {
        java.util.logging.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(java.util.logging.Level.OFF);
        java.util.logging.Logger.getLogger("org.apache.http").setLevel(java.util.logging.Level.OFF);

        URL url = new URL(str);

        WebRequest req = new WebRequest(url);
        req.setAdditionalHeader("Accept-Language", acceptLanguage);
        //req.setAdditionalHeader("Cookie", "");

        WebClient webClient =  new WebClient();
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
        if (acceptLanguage == null)
        {
            acceptLanguage = "zh-cn,zh;en-us,en-gb,en;q=0.7,*;q=0.3";
        }


        Page page = webClient.getPage(req);
        HtmlPage htmlPage = (HtmlPage) page;

        return htmlPage.asXml();
    }

    public static void main(String[] args) throws  Exception{
        System.out.println(getHtmlFromUrl("http://www.nanming.gov.cn/nmxw/zwgg/96556.shtml"));
    }
}