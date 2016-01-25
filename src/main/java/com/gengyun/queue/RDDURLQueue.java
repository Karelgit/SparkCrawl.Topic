package com.gengyun.queue;


import com.gengyun.entry.OnSparkInstanceFactory;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.CommonUtils;
import com.gengyun.utils.LogManager;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import tachyon.TachyonURI;

import java.io.IOException;
import java.io.Serializable;

/**
 * 待爬取队列
 * Created by luohongjun on 15-12-11.
 */
public class RDDURLQueue implements Serializable {
    private LogManager logger = new LogManager(RDDURLQueue.class);
   // private final static PropertyHelper helper = new PropertyHelper("db");
    private static String tachyonUrl/* = helper.getValue("tachyonUrl")*/;
//    private BlockingQueue<BaseURL> queue = new LinkedBlockingQueue<BaseURL>();


    private static int batchsize;


    public RDDURLQueue(String tachyonUrl,int batchsize) {
        this.tachyonUrl=tachyonUrl;
        this.batchsize=batchsize;
    }

    public JavaPairRDD<Text, Crawldb> nextBatch() throws IOException {
        JavaSparkContext jsc = OnSparkInstanceFactory.getSparkContext();

        jsc.hadoopConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
        JavaRDD<Crawldb> filedata = jsc.objectFile(tachyonUrl + "/toCrawl/current");
        JavaPairRDD<Text, Crawldb> originRDD = filedata.mapToPair(new PairFunction<Crawldb, Text, Crawldb>() {
            @Override
            public Tuple2<Text, Crawldb> call(Crawldb crawldb) throws Exception {
                return new Tuple2<Text, Crawldb>(new Text(crawldb.getUrl()), crawldb);
            }
        });

        JavaPairRDD<Text, Crawldb> sortedRDD = originRDD.mapToPair(new PairFunction<Tuple2<Text, Crawldb>, Long, Crawldb>() {
            @Override
            public Tuple2<Long, Crawldb> call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                return new Tuple2<Long, Crawldb>(new Long(tuple2._2().getDepthfromSeed()), tuple2._2());
            }
        }).sortByKey(true).mapToPair(new PairFunction<Tuple2<Long, Crawldb>, Text, Crawldb>() {
            @Override
            public Tuple2<Text, Crawldb> call(Tuple2<Long, Crawldb> lt) throws Exception {


                return new Tuple2<Text, Crawldb>(new Text(lt._2().getUrl()), lt._2());
            }
        });


        JavaPairRDD<Text, Crawldb> top100RDD = jsc.parallelizePairs(sortedRDD.take(batchsize));


        JavaPairRDD<Text, Crawldb> top100laterRDD = sortedRDD.union(top100RDD).mapValues(new Function<Crawldb, Crawldb>() {
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
            public Boolean call(Tuple2<Text, Crawldb> textCrawlDatumTuple2) throws Exception {
                return textCrawlDatumTuple2._2().getCount() == 1L;
            }
        });

        TachyonURI tmpout = new TachyonURI(tachyonUrl + "/toCrawl/" + String.valueOf(System.currentTimeMillis()));

        top100laterRDD.values().saveAsObjectFile(tmpout.toString());


        if (CommonUtils.exit("/toCrawl/current",tachyonUrl)) {
            if (CommonUtils.exit("/toCrawl/old",tachyonUrl)) CommonUtils.remove("/toCrawl/old",tachyonUrl);
            CommonUtils.rename("/toCrawl/current", "/toCrawl/old",tachyonUrl);
            CommonUtils.remove("/toCrawl/current",tachyonUrl);
            CommonUtils.rename(tmpout.getPath(), "/toCrawl/current",tachyonUrl);
        }


        return top100RDD;
    }

    public boolean hasMoreUrls() throws IOException {
        return CommonUtils.exit("/toCrawl",tachyonUrl);
    }

    public void putNextUrls(JavaPairRDD<Text, Crawldb> nextUrls) throws IOException {
        //queue.addAll(nextUrls);
        JavaSparkContext jsc = OnSparkInstanceFactory.getSparkContext();

        jsc.hadoopConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
        //  CommonUtils.lockToCrawl();
        if (CommonUtils.exit("/toCrawl/current",tachyonUrl)) {
            /*** 合并队列中数据 队列内部去重并存储**/
            JavaRDD<Crawldb> originRDD = jsc.objectFile(tachyonUrl + "/toCrawl/current");


            TachyonURI tmpout = new TachyonURI(tachyonUrl + "/toCrawl/" + String.valueOf(System.currentTimeMillis()));
            originRDD.union(nextUrls.values()).mapToPair(new PairFunction<Crawldb, Text, Crawldb>() {
                @Override
                public Tuple2<Text, Crawldb> call(Crawldb crawldb) throws Exception {
                    return new Tuple2<Text, Crawldb>(new Text(crawldb.getUrl()), crawldb);
                }
            }).mapValues(new Function<Crawldb, Crawldb>() {
                @Override
                public Crawldb call(Crawldb crawlDatum) throws Exception {
                    crawlDatum.setCount(1L);
                    return crawlDatum;
                }
            }).reduceByKey(new Function2<Crawldb, Crawldb, Crawldb>() {
                @Override
                public Crawldb call(Crawldb cd1, Crawldb cd2) throws Exception {
                    cd1.setCount(cd1.getCount() + cd2.getCount());
                    return cd1;
                }
            }).filter(new Function<Tuple2<Text, Crawldb>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Text, Crawldb> textCrawldbTuple2) throws Exception {
                    return textCrawldbTuple2._2().getCount() == 1L;
                }
            }).values().saveAsObjectFile(tmpout.toString());

            if (CommonUtils.exit("/toCrawl/current",tachyonUrl)) {
                if (CommonUtils.exit("/toCrawl/old",tachyonUrl)) CommonUtils.remove("/toCrawl/old",tachyonUrl);
                CommonUtils.rename("/toCrawl/current", "/toCrawl/old",tachyonUrl);
                CommonUtils.remove("/toCrawl/current",tachyonUrl);
                CommonUtils.rename(tmpout.getPath(), "/toCrawl/current",tachyonUrl);
            }


            /****end 去重****/

        } else {
            nextUrls.values().saveAsObjectFile(tachyonUrl + "/toCrawl/current");
        }
        // CommonUtils.unlockToCrawl();

    }

    public boolean cleanQueue() throws IOException {
        //queue.clear();

        /*if (tfs.exist(new TachyonURI("/toCrawl"))) {
            tfs.delete(new TachyonURI("/toCrawl"), true);
        }*/
        CommonUtils.remove("/toCrawl",tachyonUrl);
        return !CommonUtils.exit("/toCrawl",tachyonUrl); //queue.isEmpty();
    }

}
