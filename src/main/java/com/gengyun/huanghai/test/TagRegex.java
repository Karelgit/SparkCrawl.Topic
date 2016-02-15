package com.gengyun.huanghai.test;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gengyun.utils.FileReader;
import org.apache.commons.logging.LogFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 15-12-30.
 */
public class TagRegex {
    public static void main(String[] args) throws Exception{
        LogFactory.getFactory().setAttribute("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
        String url = "http://www.qxn.gov.cn/OrgArtList/QxnGov.XMJ/QxnGov.XMJ.Info/1.html";
        WebClient webClient = new WebClient(BrowserVersion.CHROME);
        webClient.getOptions().setThrowExceptionOnScriptError(false);
        webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
        webClient.getOptions().setJavaScriptEnabled(true);
        webClient.setJavaScriptTimeout(3600 * 1000);
        webClient.getOptions().setRedirectEnabled(true);
        webClient.getOptions().setTimeout(3600 * 1000);
        webClient.waitForBackgroundJavaScript(600 * 1000);
        webClient.setAjaxController(new NicelyResynchronizingAjaxController());
        HtmlPage hp = webClient.getPage(url);


        List<String> regexList = new ArrayList<>();
        try {
            regexList = FileReader.readFile("/opt/regex.config");
        } catch (Exception e) {
            e.printStackTrace();
        }
        Document document = Jsoup.parse(hp.asXml());
        document.getElementsByTag("meta").remove();
        document.getElementsByTag("style").remove();
        document.getElementsByTag("script").remove();

        for (int i = 0; i < regexList.size(); i++) {
            Pattern p = Pattern.compile("</?\\w+((\\s+\\w+(\\s*=\\s*(?:\".*?\"|'.*?'|[^'\">\\s]+))?)+\\s*|\\s*)/?>");
            String temp = document.toString().replaceAll("\\r|\\t|\\n|\\?", "");
//            System.err.println("temp:" + "\n" +temp);
            Matcher m = p.matcher(temp);
//            System.out.println("isMatched:" + "<a href=\"###\" onclick=\"javascript:showNews(20);\" class=\"pager\">".matches(regexList.get(0)));
            while (m.find()) {
                String str = m.group();
                if(m.group().matches(regexList.get(i))) {
                    System.out.println("找到的tag:" + m.group());
                }
//                System.out.println(m.group());
            }
        }
//        System.out.println("abc".matches("abc"));
    }
}
