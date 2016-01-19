package com.gengyun.huanghai;

import com.gargoylesoftware.htmlunit.WebClient;

import java.util.List;
import java.util.Map;

/**
 * 以下变量提供给动态点击
 * Created by Administrator on 2015/12/21.
 */
public class Params {


    private int k;
    private int m;
    private int layer;
    private Map<Integer,List<Tag>> tagListBlock;
    private WebClient webClient;

    private boolean firstEntry;

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public int getLayer() {
        return layer;
    }

    public void setLayer(int layer) {
        this.layer = layer;
    }

    public int getM() {
        return m;
    }

    public void setM(int m) {
        this.m = m;
    }

    public Map<Integer, List<Tag>> getTagListBlock() {
        return tagListBlock;
    }

    public void setTagListBlock(Map<Integer, List<Tag>> tagListBlock) {
        this.tagListBlock = tagListBlock;
    }

    public WebClient getWebClient() {
        return webClient;
    }

    public void setWebClient(WebClient webClient) {
        this.webClient = webClient;
    }

    public boolean isFirstEntry() {
        return firstEntry;
    }

    public void setFirstEntry(boolean firstEntry) {
        this.firstEntry = firstEntry;
    }
}
