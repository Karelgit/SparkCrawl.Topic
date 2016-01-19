package com.gengyun.metainfo;

import com.gengyun.webcomm.WebCommManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by root on 15-12-9.
 */
public class CrawlDatum implements Writable, Serializable {

    private String url;
    private int statcode;
    private String rootUrl;
    private BaseWebPage webPage;
    private String fromUrl;
    private String text;
    private String html;
    private String title;
    private long crawltime;
    private long publishtime;
    private long depthfromSeed;
    private boolean tag;
    private long count;
    private boolean fetched;

    public CrawlDatum() {
    }

    public CrawlDatum(String url, int statcode, String rootUrl, BaseWebPage webPage, String fromUrl, String text, String html, String title, long crawltime, long publishtime, long depthfromSeed, boolean tag, long count, boolean fetched) {
        this.url = url;
        this.statcode = statcode;
        this.rootUrl = rootUrl;
        this.webPage = webPage;
        this.fromUrl = fromUrl;
        this.text = text;
        this.html = html;
        this.title = title;
        this.crawltime = crawltime;
        this.publishtime = publishtime;
        this.depthfromSeed = depthfromSeed;
        this.tag = tag;
        this.count = count;
        this.fetched = fetched;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getStatcode() {
        return statcode;
    }

    public void setStatcode(int statcode) {
        this.statcode = statcode;
    }

    public String getRootUrl() {
        return rootUrl;
    }

    public void setRootUrl(String rootUrl) {
        this.rootUrl = rootUrl;
    }

    public BaseWebPage getWebPage() {
        return webPage;
    }

    public void setWebPage(BaseWebPage webPage) {
        this.webPage = webPage;
    }


    public String getFromUrl() {
        return fromUrl;
    }

    public void setFromUrl(String fromUrl) {
        this.fromUrl = fromUrl;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getHtml() {
        return html;
    }

    public void setHtml(String html) {
        this.html = html;
    }

    public long getCrawltime() {
        return crawltime;
    }

    public void setCrawltime(long crawltime) {
        this.crawltime = crawltime;
    }

    public long getPublishtime() {
        return publishtime;
    }

    public void setPublishtime(long publishtime) {
        this.publishtime = publishtime;
    }


    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public long getDepthfromSeed() {
        return depthfromSeed;
    }

    public void setDepthfromSeed(long depthfromSeed) {
        this.depthfromSeed = depthfromSeed;
    }

    public boolean isTag() {
        return tag;
    }

    public void setTag(boolean tag) {
        this.tag = tag;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public boolean isFetched() {
        return fetched;
    }

    public void setFetched(boolean fetched) {
        this.fetched = fetched;
    }

    public void downloadPageContent(WebCommManager webcomm) {
        webPage = webcomm.fetchWebPage(this);
    }


    public boolean isValid() throws MalformedURLException {


        URL url1 = new URL(getUrl());

        if (webPage != null && webPage.hasContent() && url1 != null &&
                StringUtils.isNotBlank(url1.toString()) && url1.getProtocol() != null &&
                StringUtils.isNotBlank(url1.getProtocol())) {
            if (StringUtils.equals(url1.getProtocol(), "http")) {
                return true;
            } else
                return false;
        } else {
            return false;
        }
    }

    /*@Override
    public int compareTo(CrawlDatum o) {
        return this.getUrl().compareTo(o.getUrl());
    }*/

    @Override
    public void write(DataOutput out) throws IOException {
        if (out != null) {
            Text.writeString(out, getUrl());
            out.writeInt(statcode);
            if (rootUrl == null)
                out.writeUTF("");
            else
                out.writeUTF(rootUrl);
            if (StringUtils.isEmpty(fromUrl))
                out.writeUTF("");
            else
                out.writeUTF(fromUrl);

            if (StringUtils.isEmpty(getHtml())) {
                WritableUtils.writeString(out, "");
            } else {
                WritableUtils.writeString(out, getHtml());
            }
            if (StringUtils.isEmpty(getText())) {
                WritableUtils.writeString(out, "");
            } else {
                WritableUtils.writeString(out, getText());
            }

            WritableUtils.writeVLong(out, getCrawltime());

            out.writeLong(getPublishtime());

            if (StringUtils.isEmpty(getTitle())) {
                out.writeUTF("");
            } else {
                out.writeUTF(getTitle());
            }

            out.writeLong(depthfromSeed);


            out.writeBoolean(isTag());
            out.writeLong(getCount());

            out.writeBoolean(isFetched());
        }
    }

    public Crawldb tranform() {
        Crawldb crawldb = new Crawldb();
        crawldb.setUrl(this.getUrl());
        crawldb.setStatcode(this.getStatcode());
        crawldb.setRootUrl(this.getRootUrl());
        crawldb.setFromUrl(this.getFromUrl());
        crawldb.setText(this.getText());
        crawldb.setHtml(this.getHtml());
        crawldb.setTitle(this.getTitle());
        crawldb.setCrawltime(this.getCrawltime());
        crawldb.setPublishtime(this.getPublishtime());
        crawldb.setDepthfromSeed(this.getDepthfromSeed());
        crawldb.setTag(this.isTag());
        crawldb.setCount(this.getCount());
        crawldb.setFetched(this.isFetched());
        return crawldb;
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        if (in != null) {
            url = Text.readString(in);

            setStatcode(in.readInt());
            setRootUrl(in.readUTF());
            setFromUrl(in.readUTF());

            setHtml(WritableUtils.readString(in));

            setText(WritableUtils.readString(in));

            setCrawltime(WritableUtils.readVLong(in));

            setPublishtime(in.readLong());
            setTitle(in.readUTF());
            setDepthfromSeed(in.readLong());
            setTag(in.readBoolean());
            setCount(in.readLong());
            setFetched(in.readBoolean());
        }
    }
}
