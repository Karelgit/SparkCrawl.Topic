package com.gengyun.huanghai;

import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ting on 10/15/2015.
 */
public class XpathParser {

    public static void main(String args[]) throws Exception {

        final WebClient wc = new WebClient();

        wc.getOptions().setJavaScriptEnabled(true); //启用JS解释器，默认为true
        wc.getCookieManager().setCookiesEnabled(true);

        wc.getOptions().setCssEnabled(false); //禁用css支持
        wc.getOptions().setThrowExceptionOnScriptError(true); //js运行错误时，是否抛出异常
        wc.getOptions().setTimeout(0); //设置连接超时时间 ，这里是10S。如果为0，则无限期等待
        wc.getOptions().setRedirectEnabled(true);
        wc.getOptions().setAppletEnabled(false);
        wc.getOptions().setThrowExceptionOnFailingStatusCode(true);
        wc.setAjaxController(new NicelyResynchronizingAjaxController());
        wc.getCurrentWindow().setInnerHeight(Integer.MAX_VALUE);
        HtmlPage page = wc.getPage("http://gz.hrss.gov.cn/col/col41/index.html");
        wc.waitForBackgroundJavaScript(5L * 1000);
        wc.setJavaScriptTimeout(0);
        String label = "<div class=\"default_pgBtn default_pgNext\" title=\"下页\">";


        String html = page.asXml().replaceFirst("<\\?xml version=\"1.0\" encoding=\"(.+)\"\\?>", "<!DOCTYPE html>");
        HtmlElement element = page.getFirstByXPath(getXpath(html, label).get(0));

        HtmlPage nextpage = (HtmlPage) element.click();
        wc.waitForBackgroundJavaScript(5L * 1000);
        wc.setJavaScriptTimeout(0);
        String nextpagehtml = nextpage.asXml().replaceFirst("<\\?xml version=\"1.0\" encoding=\"(.+)\"\\?>", "<!DOCTYPE html>");
        System.out.println(MinHashCompare.getHtmlSimilarPercentage(Jsoup.parse(HtmlPreprocess.delHTMLTag(html)), Jsoup.parse(HtmlPreprocess.delHTMLTag(nextpagehtml))));

    }

    public static List<String> getXpath(Document document, String label) {

        List<String> xpath = new ArrayList<String>();
        String path;
        Elements elements = getElements(document, label);

        for (Element element : elements) {
            path = getElementXpath(element);
            xpath.add(path);
        }

        return xpath;

    }


    public static List<String> getXpath(String html, String label) {

        return getXpath(Jsoup.parse(html), label);

    }

    private static Elements getElements(Document document, String label) {
        Document doc = Jsoup.parse(label);
        Element elt = doc.body().child(0);
        String tag = elt.tagName();

        for (Attribute attribute : elt.attributes()) {
            String str = "[" + attribute.getKey() + "=\"" + attribute.getValue() + "\"]";
            tag += str;
        }

        return document.select(tag);
    }

    private static String getElementXpath(Element elt) {
        String path = "";

        try {
            for (; !elt.tagName().equals("#root"); elt = elt.parent()) {
                int index = getElementIndex(elt);
                String tag = elt.tagName();

                if (index >= 1) tag += "[" + index + "]";

//                if (index > 1)
//                    tag += "[" + index + "]";

                path = "/" + tag + path;
            }
        } catch (Exception ignored) {
        }
        return path;
    }

    private static int getElementIndex(Element elt) {
        int count = 1;

        for (Node node = elt.previousElementSibling(); node != null;
             node = node.previousSibling()) {
            if (node instanceof Element) {
                Element element = (Element) node;
                if (element.tagName().equals(elt.tagName())) {
                    count++;
                }
            }
        }

        return count;
    }

}
