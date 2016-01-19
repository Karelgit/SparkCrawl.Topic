package com.gengyun.duplirm;

import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.LogManager;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * 去重
 * Created by lhj on 15-12-24.
 */
public class DuplicateRemoval implements Serializable {
    private static transient LogManager logger = new LogManager(DuplicateRemoval.class);

    public DuplicateRemoval() {
    }

    public JavaPairRDD<Text, Crawldb> duplicateremove(JavaPairRDD<Text, Crawldb> current, JavaPairRDD<Text, Crawldb> crawled) {

        JavaPairRDD<Text, Crawldb> unionedRDD = current.filter(new Function<Tuple2<Text, Crawldb>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                return !tuple2._2().isFetched();
            }
        })/*.mapValues(new Function<Crawldb, Crawldb>() {
            @Override
            public Crawldb call(Crawldb crawldb) throws Exception {
                crawldb.setCount(1L);
                return crawldb;
            }
        }).reduceByKey(new Function2<Crawldb, Crawldb, Crawldb>() {
            @Override
            public Crawldb call(Crawldb crawldb, Crawldb crawldb2) throws Exception {
                crawldb.setCount(1L);
                return crawldb;
            }
        })*/.union(crawled).mapToPair(new PairFunction<Tuple2<Text, Crawldb>, Text, Crawldb>() {
            @Override
            public Tuple2<Text, Crawldb> call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                return new Tuple2<Text, Crawldb>(new Text(tuple2._2().getUrl() + "|" + tuple2._2().getFromUrl()), tuple2._2());
            }
        }).mapValues(new Function<Crawldb, Crawldb>() {
            @Override
            public Crawldb call(Crawldb crawlDatum) throws Exception {
                crawlDatum.setCount(1L);
                return crawlDatum;
            }
        }).reduceByKey(new Function2<Crawldb, Crawldb, Crawldb>() {
            @Override
            public Crawldb call(Crawldb crawlDatum, Crawldb crawlDatum2) throws Exception {
                crawlDatum.setCount(crawlDatum.getCount() + crawlDatum2.getCount());
                return crawlDatum;
            }
        }).filter(new Function<Tuple2<Text, Crawldb>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Text, Crawldb> textCrawldbTuple2) throws Exception {
                return !textCrawldbTuple2._2().isFetched();
            }
        });


        JavaPairRDD<Text, Crawldb> newRDD = unionedRDD.filter(new Function<Tuple2<Text, Crawldb>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Text, Crawldb> tuple2) throws Exception {

                if (tuple2._2().getCount() == 1L) {
                    logger.logInfo(tuple2._1() + "\t" + tuple2._2().getFromUrl() + "\thas not been seen.");
                    return true;
                } else {
                    logger.logInfo(tuple2._1() + "\t" + "\t" + tuple2._2().getFromUrl() + "\thas seen before.");
                    return false;
                }
            }
        }).mapToPair(new PairFunction<Tuple2<Text, Crawldb>, Text, Crawldb>() {
            @Override
            public Tuple2<Text, Crawldb> call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                return new Tuple2<Text, Crawldb>(new Text(tuple2._1().toString().split("\\|")[0]), tuple2._2());
            }
        });

        return newRDD;
    }

}
