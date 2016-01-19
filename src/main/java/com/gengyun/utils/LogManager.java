package com.gengyun.utils;

/**
 * Created by hadoop on 2015/11/9.
 */

import com.gengyun.crawler.OnSparkWorkflowManager;
import com.gengyun.duplirm.DuplicateRemoval;
import com.gengyun.duplirm.RedisDuplirm;
import com.gengyun.entry.ConfigLoader;
import com.gengyun.metainfo.BaseURL;
import com.gengyun.queue.RDDRedisToCrawlQue;
import com.gengyun.urlfilter.FilterEnforcer;
import com.gengyun.urlfilter.InMemroySeenUrlFilter;
import com.gengyun.urlfilter.RDDPostExpansionFilterEnforcer;
import com.gengyun.urlfilter.UrlDepthFilter;
import com.gengyun.urlidentifier.HUBasedURLIdentifier;
import com.gengyun.urlidentifier.JsoupBasedURLIdentifier;
import com.gengyun.urlidentifier.URLIdentifier;
import com.gengyun.webcomm.HtmlUnitWebCommManager;
import com.gengyun.webcomm.JsoupWebCommManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class LogManager implements Serializable {
    private Logger logger;
    private String className;

    public LogManager(Class callingClass) {

        this.className = callingClass.getName();
        //  this.logger = Logger.getLogger(className);
    }

    public void logError(String msg) {
        if (shouldThisClassLog(className)) {
            System.out.println(msg);
            //logger.error(msg);
        }
    }

    public void logInfo(String msg) {
        if (shouldThisClassLog(className)) {
            System.out.println(msg);
            //logger.info(msg);
        }
    }

    public void logDebug(String msg) {
        if (shouldThisClassLog(className)) {
            System.out.println(msg);
            //logger.debug(msg);
        }
    }

    private static Set<String> loggingClasses = new HashSet<String>(Arrays.asList(
            URLIdentifier.class.getName(),
            JsoupBasedURLIdentifier.class.getName(),
            ConfigLoader.class.getName(),
            BaseURL.class.getName(),
            RDDPostExpansionFilterEnforcer.class.getName(),
            FilterEnforcer.class.getName(),
            InMemroySeenUrlFilter.class.getName(),
            JsoupWebCommManager.class.getName(),
            HtmlUnitWebCommManager.class.getName(),
            HUBasedURLIdentifier.class.getName(),
            OnSparkWorkflowManager.class.getName(),
            UrlDepthFilter.class.getName(),
            DuplicateRemoval.class.getName(),
            RDDRedisToCrawlQue.class.getName(),
            RedisDuplirm.class.getName()));

    private static boolean shouldThisClassLog(String className) {
        if (loggingClasses.contains(className)) {
            return true;
        } else {
            return false;
        }
    }
}
