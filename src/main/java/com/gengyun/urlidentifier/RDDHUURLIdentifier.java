package com.gengyun.urlidentifier;

import com.gengyun.metainfo.Crawldb;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * @author luohongjun
 *         htmlunit抽取外链
 *         Created by hadoop on 2015/11/17.
 */
public class RDDHUURLIdentifier implements Serializable {

    private DOMContentUtils utils;
    private HUBasedURLIdentifier iden;

    public RDDHUURLIdentifier(SparkConf sparkConf, HashSet<String> postfix) {
        utils = new DOMContentUtils(sparkConf);
        iden = new HUBasedURLIdentifier(utils, postfix);
    }


    /*public FlatMapFunction<BaseURL, BaseURL> extractUrls() {
        FlatMapFunction<BaseURL, BaseURL> result = new FlatMapFunction<BaseURL, BaseURL>() {
            @Override
            public Iterable<BaseURL> call(BaseURL url) throws Exception {
                HUBasedURLIdentifier iden = new HUBasedURLIdentifier(utils);
                return iden.extractUrls(url);
            }
        };
        return result;
    }*/


    public PairFlatMapFunction<Tuple2<Text, Crawldb>, Text, Crawldb> extractUrls() {
        PairFlatMapFunction<Tuple2<Text, Crawldb>, Text, Crawldb> result = new PairFlatMapFunction<Tuple2<Text, Crawldb>, Text, Crawldb>() {
            @Override
            public Iterable<Tuple2<Text, Crawldb>> call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                List<Tuple2<Text, Crawldb>> list = iden.extractCrawlDatumUrls(tuple2);
                return list;
            }
        };

        return result;
    }

    public PairFlatMapFunction<Iterator<Tuple2<Text, Crawldb>>, Text, Crawldb> extractUrlsbyPart() {
        PairFlatMapFunction<Iterator<Tuple2<Text, Crawldb>>, Text, Crawldb> result = new PairFlatMapFunction<Iterator<Tuple2<Text, Crawldb>>, Text, Crawldb>() {
            @Override
            public Iterable<Tuple2<Text, Crawldb>> call(Iterator<Tuple2<Text, Crawldb>> tuple2Iterator) throws Exception {
                return iden.extractCrawlDatumUrls(tuple2Iterator);
            }
        };

        return result;
    }

}
