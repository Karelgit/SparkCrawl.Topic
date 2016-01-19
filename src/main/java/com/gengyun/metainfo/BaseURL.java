package com.gengyun.metainfo;

/**
 * Created by hadoop on 2015/11/9.
 */

import com.gengyun.utils.LogManager;
import com.gengyun.webcomm.WebCommManager;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.net.URL;

public class BaseURL implements Serializable {
    private transient LogManager logger = new LogManager(BaseURL.class);

    private URL url;
    private int depthFromSeed = -1;
    private BaseWebPage page;

    public URL getParentUrl() {
        return parentUrl;
    }

    public void setParentUrl(URL parentUrl) {
        this.parentUrl = parentUrl;
    }

    private URL parentUrl;


    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    private String title;

    public BaseURL(URL input) {
        this.url = input;
    }

    public Long count;

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
    /*public BaseURL(URL input, BaseWebPage page){
        this.url = input;
        this.page = page;
    }*/

    public void setDepthFromSeed(int currentDepth) {
        if (depthFromSeed == -1 || depthFromSeed > currentDepth) {
            depthFromSeed = currentDepth;
        }
    }

    public int getDepthFromSeed() {
        return depthFromSeed;
    }

    public URL getUrl() {
        return url;
    }

    public void setUrl(URL url) {
        this.url = url;
    }

    public BaseWebPage getPageContent() {
        return this.page;
    }

    public void downloadPageContent(WebCommManager webcomm) {
        //page = webcomm.fetchPage(this);
    }

    public boolean isValid() {


        if (page != null && page.hasContent() && url != null &&
                StringUtils.isNotBlank(url.toString()) && url.getProtocol() != null &&
                StringUtils.isNotBlank(url.getProtocol())) {
            if (StringUtils.equals(url.getProtocol(), "http")) {
                return true;
            } else
                return false;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((url == null) ? 0 : url.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BaseURL other = (BaseURL) obj;
        if (url == null) {
            if (other.url != null)
                return false;
        } else if (!url.equals(other.url))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "BaseURL [url=" + url + ", depthFromSeed=" + depthFromSeed + "]";
    }
}
