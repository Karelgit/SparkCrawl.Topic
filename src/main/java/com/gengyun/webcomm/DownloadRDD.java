package com.gengyun.webcomm;

import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gengyun.entry.OnSparkInstanceFactory;
import com.gengyun.metainfo.Crawldb;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple3;

import java.io.Serializable;

/**
 * Created by root on 15-12-30.
 */
public class DownloadRDD implements Serializable {
    public DownloadRDD() {
    }

    public JavaRDD<Tuple3<Text, Crawldb, HtmlPage>> download(JavaPairRDD<Text, Crawldb> currBatch) {
        return currBatch
                .map(OnSparkInstanceFactory.getHtmlUnitDownload().download())
                .filter(OnSparkInstanceFactory.getProtocolFilter().filter());
    }


}
