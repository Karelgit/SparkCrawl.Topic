package com.gengyun.metainfo;


import java.io.Serializable;

/**
 * Created by lhj on 15-12-15.
 */
public class Crawldb implements /*WritableComparable<Crawldb>,*/ Serializable {
    private final static long serialVersionUID = -2344403674643228206L;
    private String tid;
    private String url;
    private int statcode;
    private int passed;//遍数
    private String type;
    private String rootUrl;
    private String fromUrl;
    private String text;
    private String html;
    private String title;
    private long crawltime;
    private long publishtime;
    private long depthfromSeed;
    private boolean tag;//true:文章,false:导航
    private long count;
    private boolean fetched;

    public Crawldb() {
    }

    public Crawldb(String url, int statcode, String rootUrl, String fromUrl, String text, String html, String title, long crawltime, long publishtime, long depthfromSeed, boolean tag, long count, boolean fetched) {
        this.url = url;
        this.statcode = statcode;
        this.rootUrl = rootUrl;
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


    public String getTid() {
        return tid;
    }

    public void setTid(String tid) {
        this.tid = tid;
    }

    public int getPassed() {
        return passed;
    }

    public void setPassed(int passed) {
        this.passed = passed;
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

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /*
    @Override
    public void readFields(DataInput in) throws IOException {
        if (in != null) {
            setUrl(in.readUTF());

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

    @Override
    public void write(DataOutput out) throws IOException {
        if (out != null) {
            out.writeUTF(getUrl());

            out.writeInt(getStatcode());
            if (getRootUrl() == null)
                out.writeUTF("");
            else
                out.writeUTF(getRootUrl());
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

    @Override
    public int compareTo(Crawldb that) {
        return this.getUrl().compareTo(that.getUrl());
    }*/
}
