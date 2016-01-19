package com.gengyun.entry;


import com.gengyun.metainfo.BaseWebPage;
import com.gengyun.metainfo.HtmlUnitDocWebPage;
import com.gengyun.metainfo.JsoupDocWebPage;
import com.gengyun.webcomm.HtmlUnitWebCommManager;
import com.gengyun.webcomm.JsoupWebCommManager;
import com.gengyun.webcomm.WebCommManager;

/**
 * 爬虫构造工厂
 * Created by lhj on 2015/11/9.
 */
public class InstanceFactory {

    //private static URLQueue nextQueue = new InMemorySingleProcessBasicQueue();
    private static WebCommManager webcomm = new JsoupWebCommManager();
    // private static URLIdentifier urlIdentifier = new JsoupBasedURLIdentifier();
    private static WebCommManager huwebcomm = new HtmlUnitWebCommManager();

    public static WebCommManager getWebCommManager() {
        return webcomm;
    }


    public static BaseWebPage getOneWebPage() {
        return new JsoupDocWebPage();
    }

    public static BaseWebPage getOneHtmlunitWebPage() {
        return new HtmlUnitDocWebPage();
    }

    public static WebCommManager getHUWebCommManager() {
        return huwebcomm;
    }



}
