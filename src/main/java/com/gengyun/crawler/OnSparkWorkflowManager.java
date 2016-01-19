package com.gengyun.crawler;


import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gengyun.duplirm.RedisDuplirm;
import com.gengyun.entry.OnSparkInstanceFactory;
import com.gengyun.flter.DomainFilter;
import com.gengyun.flter.ProtocolFilter;
import com.gengyun.huanghai.ClickFunction;
import com.gengyun.huanghai.Params;
import com.gengyun.lsc.analysis.analysis.TextAnalysis;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.queue.RDDRedisCrawledQue;
import com.gengyun.queue.RDDRedisToCrawlQue;
import com.gengyun.urlfilter.RDDPreExpansionFilterEnforcer;
import com.gengyun.utils.*;
import com.gengyun.webcomm.DownloadRDD;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import redis.clients.jedis.Jedis;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

/**
 * 爬虫主程序
 * Created by lhj on 2015/11/9.
 */
public class OnSparkWorkflowManager implements Serializable {
    private LogManager logger = new LogManager(OnSparkWorkflowManager.class);

    //待抓取队列
    //private RDDURLQueue nextQueue = OnSparkInstanceFactory.getNextURLQueueInstance();
    private RDDRedisToCrawlQue nextQueue = OnSparkInstanceFactory.getRedisToCrawlQue();


    //已抓取队列
    //private RDDCrawledQueue crawledQueue = OnSparkInstanceFactory.getRddCrawledQueue();
    private RDDRedisCrawledQue crawledQueue = OnSparkInstanceFactory.getRddRedisCrawledQue();


    //下载
    // private HtmlUnitDownload downloadplugin = OnSparkInstanceFactory.getHtmlUnitDownload();

    private DownloadRDD downloadRDD = OnSparkInstanceFactory.getDownloadRdd();

    //深度过滤
    private RDDPreExpansionFilterEnforcer preExpansionfilterEnforcer = OnSparkInstanceFactory.getPreExpansionFilterEnforcer();

    //协议过滤
    private ProtocolFilter protocolFilter = OnSparkInstanceFactory.getProtocolFilter();

    //去重
    //private DuplicateRemoval duplicateRemoval = OnSparkInstanceFactory.getDuplicateRemoval();
    private RedisDuplirm redisDuplirm = OnSparkInstanceFactory.getRedisDuplirm();


    private TextAnalysis textAnalysis = OnSparkInstanceFactory.getTextAnalysis();

    private JedisPoolUtils jedisPoolUtils = OnSparkInstanceFactory.getJedisPoolUtils();

    private ClickFunction clickFunction = OnSparkInstanceFactory.getClickFunction();

    private String taskid;

    public OnSparkWorkflowManager(String taskid) {
        this.taskid = taskid;
    }

    public void crawl(List<Tuple2<Text, Crawldb>> seeds, String tid,String starttime, int pass) throws IOException {
        PropertyHelper helper = new PropertyHelper("db");
        Properties properties = new Properties();
        properties.setProperty("user", helper.getValue("mysql.user"));
        properties.setProperty("password", helper.getValue("mysql.password"));
        properties.setProperty("useUnicode", "true");
        properties.setProperty("characterEncoding", "utf8");
        properties.setProperty("autoReconnect", "true");
        properties.setProperty("rewriteBatchedStatements", "true");
        JavaSparkContext jsc = OnSparkInstanceFactory.getSparkContext();

        final Broadcast<JedisPoolUtils> jedisPoolUtilsBroadcast = jsc.broadcast(jedisPoolUtils);

        final Broadcast<String> _tidbc = jsc.broadcast(tid);

        nextQueue.putNextUrls(jsc.parallelizePairs(seeds), jedisPoolUtilsBroadcast, _tidbc);

        String dburl = helper.getValue("db.url");
        final String table = helper.getValue("db.table");
        String tachyonUrl = helper.getValue("tachyonUrl");
        TachyonURI domainURI = new TachyonURI(tachyonUrl + "/SparkCrawler/" + tid +starttime+ "/domains");
        TachyonFS tfs = TachyonFS.get(new TachyonURI(tachyonUrl));

        jsc.hadoopConfiguration().set("fs.tachyon.impl", "tachyon.hadoop.TFS");
        JavaRDD<String> domainRDD = jsc.textFile(domainURI.toString());
        final HashSet<String> domains = new HashSet<String>(domainRDD.collect());

        //域名过滤
        DomainFilter domainFilter = new DomainFilter(domains);


        long start1 = 0;
        long end1 = 0;
        String time1 = "";

        long start2 = 0;
        long end2 = 0;
        String time2 = "";

        long start3 = 0;
        long end3 = 0;
        String time3 = "";

        long start4 = 0;
        long end4 = 0;
        String time4 = "";
        long start5 = 0;
        long end5 = 0;
        String time5 = "";

        long start6 = 0;
        long end6 = 0;
        String time6 = "";
        long start7 = 0;
        long end7 = 0;
        String time7 = "";
        long start8 = 0;
        long end8 = 0;
        String time8 = "";
        long start9 = 0;
        long end9 = 0;
        String time9 = "";
        long start10 = 0;
        long end10 = 0;
        String time10 = "";
        long start11 = 0;
        long end11 = 0;
        String time11 = "";
        long start12 = 0;
        long end12 = 0;
        String time12 = "";
        long start13 = 0;
        long end13 = 0;
        String time13 = "";

        String time = "";

        long round = 0;


        Jedis jedis = jedisPoolUtils.getJedisPool().getResource();
        jedis.del("sparkcrawler");
        if (jedis != null) {
            jedisPoolUtils.getJedisPool().returnResource(jedis);
        }

        while (shouldContinue()) {

            start1 = System.currentTimeMillis();
            JavaPairRDD<Text, Crawldb> currBatch = nextQueue.nextBatch(jedisPoolUtilsBroadcast, _tidbc);
            end1 = System.currentTimeMillis();
            time1 = String.valueOf(end1 - start1);


            start2 = System.currentTimeMillis();
            JavaRDD<Tuple3<Text, Crawldb, HtmlPage>> downloaded = downloadRDD.download(currBatch);


            end2 = System.currentTimeMillis();


            time2 = String.valueOf(end2 - start2);
            /*****动态链接的抽取********/
            start3 = System.currentTimeMillis();
            JavaRDD<Tuple4<Text, Crawldb, HtmlPage, Params>> tagBlockList = downloaded.map(clickFunction.takeTagListBlock());
            end3 = System.currentTimeMillis();
            time3 = String.valueOf(end3 - start3);

            start4 = System.currentTimeMillis();
            JavaPairRDD<Text, Crawldb> traverseClickRDD = tagBlockList.flatMapToPair(clickFunction.takeTraverseClick());
            end4 = System.currentTimeMillis();

            time4 = String.valueOf(end4 - start4);

            logger.logInfo("downloaded web pages");

            /************end 动态链接的抽取******************/

            /*****抽取外链并过滤后缀********/

            start5 = System.currentTimeMillis();
            JavaPairRDD<Text, Crawldb> baseURLJavaRDD = traverseClickRDD.flatMapToPair(textAnalysis.analysis());
            end5 = System.currentTimeMillis();
            time5 = String.valueOf(end5 - start5);

            start6 = System.currentTimeMillis();
            JavaPairRDD<Text, Crawldb> result = baseURLJavaRDD.filter(new Function<Tuple2<Text, Crawldb>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Text, Crawldb> textCrawldbTuple2) throws Exception {
                    if (textCrawldbTuple2._2().isFetched()) {
                        logger.logInfo("crawled:  " + textCrawldbTuple2._1());
                        return true;
                    } else {
                        return false;
                    }
                }
            });
            end6 = System.currentTimeMillis();
            time6 = String.valueOf(end6 - start6);

            start7 = System.currentTimeMillis();
            crawledQueue.putRDD(result, jedisPoolUtilsBroadcast, _tidbc);
            end7 = System.currentTimeMillis();
            time7 = String.valueOf(end7 - start7);

            start8 = System.currentTimeMillis();

            TachyonURI tmpout = new TachyonURI(tachyonUrl + "/SparkCrawler/" + tid +starttime+ "/data/" + String.valueOf(round));
            result.values().saveAsObjectFile(tmpout.toString());

            if (!tfs.exist(new TachyonURI(tmpout.getPath() + "/.sync")))
                tfs.createFile(new TachyonURI(tmpout.getPath() + "/.sync"));
           /* JavaRDD<Row> rowJavaRDD = result.map(new Function<Tuple2<Text, Crawldb>, Row>() {
                @Override
                public Row call(Tuple2<Text, Crawldb> t) throws Exception {
                    return RowFactory.create(t._2().getUrl(), t._2().getFromUrl(), String.valueOf(t._2().getCrawltime()), String.valueOf(t._2().getDepthfromSeed()), String.valueOf(t._2().isFetched()), t._2().getStatcode(), t._2().getRootUrl(), t._2().getText(), t._2().getHtml(), t._2().getTitle(), t._2().getPublishtime(), t._2().isTag());
                }
            });

            DataFrame df = OnSparkInstanceFactory.getSQLContext().createDataFrame(rowJavaRDD, OnSparkInstanceFactory.getSchema());
            DataFrameWriter dataFrameWriter = df.write().mode(SaveMode.Append);
            dataFrameWriter.jdbc(dburl, table, properties);*/


            end8 = System.currentTimeMillis();
            time8 = String.valueOf(end8 - start8);

            /******************end 抽取外部链接**********************/


            /**********************start 去重********************************/
            start9 = System.currentTimeMillis();
            end9 = System.currentTimeMillis();
            time9 = String.valueOf(end9 - start9);

            start10 = System.currentTimeMillis();
            JavaPairRDD<Text, Crawldb> newRDD = redisDuplirm.duplicateremove(baseURLJavaRDD, jedisPoolUtilsBroadcast, _tidbc);
            end10 = System.currentTimeMillis();
            time10 = String.valueOf(end10 - start10);

            /***********************end 去重***************************************/

            /***********************start 域名过滤**************************************/
            start11 = System.currentTimeMillis();
            JavaPairRDD<Text, Crawldb> curBatchCrawlResult = newRDD.filter(protocolFilter.filterCrawldb()).filter(domainFilter.filterFromUrl());
            end11 = System.currentTimeMillis();
            time11 = String.valueOf(end11 - start11);
            /***********************end 域名过滤**********************************************/


            /***************************start 深度过滤，并加入待爬取队列***************************************/
            start12 = System.currentTimeMillis();
            JavaPairRDD<Text, Crawldb> zuizhongRDD = curBatchCrawlResult.filter(preExpansionfilterEnforcer.filterDepth());
            end12 = System.currentTimeMillis();

            time12 = String.valueOf(end12 - start12);

            start13 = System.currentTimeMillis();

            nextQueue.putNextUrls(zuizhongRDD.filter(domainFilter.filterUrl()), jedisPoolUtilsBroadcast, _tidbc);
            end13 = System.currentTimeMillis();
            time13 = String.valueOf(end13 - start13);

            round++;

            time = time1 + " | "
                    + time2 + " | "
                    + time3 + " | "
                    + time4 + " | "
                    + time5 + " | "
                    + time6 + " | "
                    + time7 + " | "
                    + time8 + " | "
                    + time9 + " | "
                    + time10 + " | "
                    + time11 + " | "
                    + time12 + " | "
                    + time13;


            Jedis jedis2 = jedisPoolUtils.getJedisPool().getResource();
            jedis2.select(0);
            jedis2.hset("sparkcrawler", String.valueOf(round), time);
            if (jedis2 != null) {
                jedisPoolUtils.getJedisPool().returnResource(jedis2);
            }
            /*************************************************************************/
            //PaceKeeper.pause();
        }

        Jedis jedis1 = jedisPoolUtils.getJedisPool().getResource();
        jedis1.select(1);
        jedis1.del(jedis1.keys("sparkcrawler::Crawled::" + tid + "::*").toArray(new String[0]));
        jedis1.select(0);
        jedis1.del("sparkcrawler::ToCrawl::" + tid);
        jedis1.close();
    }

    protected boolean shouldContinue() throws IOException {
        boolean rs = nextQueue.hasMoreUrls(taskid);
        logger.logInfo("should continue: " + rs);
        return rs;
    }
}
