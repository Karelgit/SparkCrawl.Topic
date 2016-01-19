package com.gengyun.metainfo;

import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by hadoop on 2015/11/17.
 */
public class HtmlUnitDocWebPage extends BaseWebPage {
    public HtmlPage getDoc() {
        return doc;
    }

    public void setDoc(HtmlPage doc) {
        this.doc = doc;
    }

    private HtmlPage doc;


    @Override
    public String toString() {
        return "HtmlUnitDocWebPage[pageHtml=]" + doc.toString() + "]";
    }

    @Override
    public boolean hasContent() {
        return doc != null && StringUtils.isNoneEmpty(doc.asText()) && StringUtils.isNoneBlank(doc.asText());
    }


    @Override
    public String getPageHtml() {
        return doc.asXml().replaceFirst("<\\?xml version=\"1.0\" encoding=\"(.+)\"\\?>", "<!DOCTYPE html>");
    }

    @Override
    public HtmlPage getHtmlPage() {
        return getDoc();
    }
}
