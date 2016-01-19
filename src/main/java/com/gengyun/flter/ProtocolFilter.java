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

/**
 * 协议过滤
 * Created by lhj on 15-12-24.
 */
public class ProtocolFilter implements Serializable {


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
                            if (StringUtils.equals(url1.getProtocol(), "http")) {
                                return true;
                            } else
                                return false;
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
                String host = url.getHost();
                if (StringUtils.isNotEmpty(host)) {
                    if (StringUtils.equals(protocol, "http"))
                        return true;
                    else
                        return false;
                } else
                    return false;
            }
        };

        return result;
    }
}
