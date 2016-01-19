package com.gengyun.metainfo;

/**
 * Created by hadoop on 2015/11/9.
 */

import com.gargoylesoftware.htmlunit.html.HtmlPage;

import java.io.Serializable;

public abstract class BaseWebPage implements Serializable {

    abstract public boolean hasContent();

    abstract public String getPageHtml();

    abstract public HtmlPage getHtmlPage();
}
