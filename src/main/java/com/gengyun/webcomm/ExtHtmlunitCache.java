package com.gengyun.webcomm;

import com.gargoylesoftware.htmlunit.Cache;
import com.gargoylesoftware.htmlunit.WebResponse;
import org.apache.commons.lang.StringUtils;

/**
 * Created by hadoop on 2015/11/17.
 */
public class ExtHtmlunitCache extends Cache{
    protected boolean isDynamicContent(final WebResponse response) {
        final String cacheControl = response.getResponseHeaderValue("Cache-Control");
        if (StringUtils.isNotBlank(cacheControl) && cacheControl.toLowerCase().indexOf("max-age") > -1) {
            return false;
        }

        return super.isDynamicContent(response);
    }
}
