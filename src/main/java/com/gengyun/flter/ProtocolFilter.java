package com.gengyun.flter;

import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gengyun.metainfo.Crawldb;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.net.URL;
import java.util.HashSet;
import java.util.List;

/**
 * 协议过滤
 * Created by lhj on 15-12-24.
 */
public class ProtocolFilter implements Serializable {


    public List<String> getProtocols() {
        return protocols;
    }

    public void setProtocols(List<String> protocols) {
        this.protocols = protocols;
    }

    private List<String> protocols;

    public ProtocolFilter() {
    }

    public Function<Tuple3<Text, Crawldb, HtmlPage>, Boolean> filter() {
        Function<Tuple3<Text, Crawldb, HtmlPage>, Boolean> result = new Function<Tuple3<Text, Crawldb, HtmlPage>, Boolean>() {
            @Override
            public Boolean call(Tuple3<Text, Crawldb, HtmlPage> tuple3) throws Exception {
                if (tuple3 != null) {
                    URL url1 = new URL(tuple3._1().toString());
                    if (tuple3._3() == null) {
                        return false;
                    } else {
                        if (url1 != null && StringUtils.isNotBlank(url1.toString()) && url1.getProtocol() != null &&
                            StringUtils.isNotBlank(url1.getProtocol())) {
                            boolean flag = false;
                            for (String protocol : protocols) {
                                flag = StringUtils.equals(url1.getProtocol(), protocol.substring(0,protocol.indexOf(":"))) || flag;
                            }
                            if (flag ==true) {
                                return true;
                            } else {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                } else {
                    return false;
                }
            }
        };

        return result;
    }

    public Function<Tuple2<Text, Crawldb>, Boolean> filterCrawldb() {
        Function<Tuple2<Text, Crawldb>, Boolean> result = new Function<Tuple2<Text, Crawldb>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                URL url = new URL(tuple2._1().toString());
                String protocol = url.getProtocol();
                System.out.println("协议是：" + protocol);
                String host = url.getHost();
                if (StringUtils.isNotEmpty(host)) {
                    boolean flag = false;
                    for(String fiterProtocol : protocols)    {
                        flag = StringUtils.equals(protocol, fiterProtocol.substring(0,fiterProtocol.indexOf(":"))) || flag;
                    }
                    if (flag == true)   {
                        return true;
                    }else   {
                        return false;
                    }
                } else
                    return false;
            }
        };

        return result;
    }
}
