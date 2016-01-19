package com.gengyun.queue;

import com.gengyun.entry.OnSparkInstanceFactory;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.CommonUtils;
import com.gengyun.utils.LogManager;
import com.gengyun.utils.PropertyHelper;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import tachyon.TachyonURI;

import java.io.IOException;
import java.io.Serializable;

/**
 * 已爬取队列
 * Created by luohongjun on 15-12-11.
 */
public class RDDCrawledQueue implements Serializable {
    private LogManager logger = new LogManager(RDDURLQueue.class);
    private final static PropertyHelper helper = new PropertyHelper("db");
    private final static String tachyonUrl = helper.getValue("tachyonUrl");

    public void putRDD(JavaPairRDD<Text, Crawldb> crawledRDD) throws IOException {
        JavaSparkContext jsc = OnSparkInstanceFactory.getSparkContext();

        if (CommonUtils.exit("/Crawled")) {
            JavaRDD<Crawldb> fileRDD = jsc.objectFile(tachyonUrl + "/Crawled/current");
            JavaPairRDD<Text, Crawldb> originRDD = fileRDD.mapToPair(new PairFunction<Crawldb, Text, Crawldb>() {
                @Override
                public Tuple2<Text, Crawldb> call(Crawldb crawldb) throws Exception {
                    return new Tuple2<Text, Crawldb>(new Text(crawldb.getUrl()), crawldb);
                }
            });
            TachyonURI tmpout = new TachyonURI(tachyonUrl + "/Crawled/" + String.valueOf(System.currentTimeMillis()));

            /**** 合并已爬取数据，并去重 存储******/
            originRDD.union(crawledRDD).reduceByKey(new Function2<Crawldb, Crawldb, Crawldb>() {
                @Override
                public Crawldb call(Crawldb cd1, Crawldb cd2) throws Exception {
                    cd1.setCount(cd1.getCount() + cd2.getCount());
                    return cd1;
                }
            }).values().saveAsObjectFile(tmpout.toString());

            if (CommonUtils.exit("/Crawled/current")) {
                if (CommonUtils.exit("/Crawled/old")) CommonUtils.remove("/Crawled/old");
                CommonUtils.rename("/Crawled/current", "/Crawled/old");
                CommonUtils.rename(tmpout.getPath(), "/Crawled/current");
            }
            /*********end 存储*****/
        } else {
            crawledRDD.values().saveAsObjectFile(tachyonUrl + "/Crawled/current");
        }

    }

    public JavaPairRDD<Text, Crawldb> deQueRDD() throws IOException {
        JavaSparkContext jsc = OnSparkInstanceFactory.getSparkContext();
        JavaRDD<Crawldb> fileRDD = jsc.objectFile(tachyonUrl + "/Crawled/current");

        JavaPairRDD<Text, Crawldb> originRDD = fileRDD.mapToPair(new PairFunction<Crawldb, Text, Crawldb>() {
            @Override
            public Tuple2<Text, Crawldb> call(Crawldb crawldb) throws Exception {
                return new Tuple2<Text, Crawldb>(new Text(crawldb.getUrl()), crawldb);
            }
        });
        return originRDD;
    }

}
