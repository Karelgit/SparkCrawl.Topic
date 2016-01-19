package com.gengyun.huanghai;

/**存储标签的xpath和status(0、为触发；1、正在触发；2、已触发)
 * Created by Karel on 2015/11/17.
 */
public class Tag {
    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }


    public String getXpath() {
        return xpath;
    }

    public void setXpath(String xpath) {
        this.xpath = xpath;
    }

    String xpath;
    Integer status;
}
