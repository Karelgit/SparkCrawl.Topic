package com.gengyun.crawler;


import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gengyun.duplirm.RedisDuplirm;
import com.gengyun.entry.OnSparkInstanceFactory;
import com.gengyun.flter.DomainFilter;
import com.gengyun.flter.PostFixFilter;
import com.gengyun.flter.ProtocolFilter;
import com.gengyun.huanghai.ClickFunction;
import com.gengyun.huanghai.Params;
import com.gengyun.lsc.analysis.analysis.TextAnalysis;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.queue.RDDRedisCrawledQue;
import com.gengyun.queue.RDDRedisToCrawlQue;
import com.gengyun.urlfilter.RDDPreExpansionFilterEnforcer;
import com.gengyun.utils.JSONUtil;
import com.gengyun.utils.JedisPoolUtils;
import com.gengyun.utils.LogManager;
import com.gengyun.webcomm.DownloadRDD;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import redis.clients.jedis.Jedis;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

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

    //后缀过滤
    private PostFixFilter postFixFilter = OnSparkInstanceFactory.getPostFixFilter();

    //去重
    //private DuplicateRemoval duplicateRemoval = OnSparkInstanceFactory.getDuplicateRemoval();
    private RedisDuplirm redisDuplirm = OnSparkInstanceFactory.getRedisDuplirm();


    private TextAnalysis textAnalysis = OnSparkInstanceFactory.getTextAnalysis();

   // private JedisPoolUtils jedisPoolUtils = OnSparkInstanceFactory.getJedisPoolUtils();

    private ClickFunction clickFunction = OnSparkInstanceFactory.getClickFunction();

    private String taskid;

    private String tachyonUrl;

    public OnSparkWorkflowManager(String taskid,String tachyonUrl) {
        this.taskid = taskid;

        this.tachyonUrl=tachyonUrl;


    }

    public void crawl(List<Tuple2<Text, Crawldb>> seeds, String tid,String starttime, int pass,String redisIP,String redisPort) throws IOException {
        //PropertyHelper helper = new PropertyHelper("db");

        JavaSparkContext jsc = OnSparkInstanceFactory.getSparkContext();


        final Broadcast<String> redisip=jsc.broadcast(redisIP);
        final Broadcast<Integer> redisport=jsc.broadcast(Integer.valueOf(redisPort));

        JedisPoolUtils jedisPoolUtils=new JedisPoolUtils();


        final Broadcast<JedisPoolUtils> jedisPoolUtilsBroadcast = jsc.broadcast(jedisPoolUtils);


        final Broadcast<String> _tidbc = jsc.broadcast(tid);

        nextQueue.putNextUrls(jsc.parallelizePairs(seeds), jedisPoolUtilsBroadcast, _tidbc);

        //String tachyonUrl = helper.getValue("tachyonUrl");
        TachyonURI domainURI = new TachyonURI(tachyonUrl + "/SparkCrawler/" + tid +starttime+ "/domains");
        TachyonFS tfs = TachyonFS.get(new TachyonURI(tachyonUrl));

        jsc.hadoopConfiguration().set("fs.tachyon.impl", "tachyon.hadoop.TFS");
        JavaRDD<String> domainRDD = jsc.textFile(domainURI.toString());
        final HashSet<String> domains = new HashSet<String>(domainRDD.collect());

        //域名过滤
        DomainFilter domainFilter = new DomainFilter(domains);

        long round = 0;

        Jedis jedis = jedisPoolUtils.getJedisPool().getResource();
        jedis.del("sparkcrawler");
        if (jedis != null) {
            jedisPoolUtils.getJedisPool().returnResource(jedis);
        }

        Jedis jedis2 = jedisPoolUtils.getJedisPool().getResource();
        int depth;
        int crawler_amount =0;
        int amount;
        Long time;
        Map status_map = new HashMap<>();
        Map data_map = new HashMap<>();

        while (shouldContinue(jedisPoolUtils)) {
            JavaPairRDD<Text, Crawldb> currBatch = nextQueue.nextBatch(jedisPoolUtilsBroadcast, _tidbc).cache();
            depth = ((int)currBatch.toArray().get(0)._2().getDepthfromSeed());
            amount = currBatch.toArray().size();
            JavaRDD<Tuple3<Text, Crawldb, HtmlPage>> downloaded = downloadRDD.download(currBatch);

            /*****动态链接的抽取********/
            JavaRDD<Tuple4<Text, Crawldb, HtmlPage, Params>> initParams = downloaded.map(clickFunction.initParams());

            JavaPairRDD<Text, Crawldb> traverseClickRDD = initParams.flatMapToPair(clickFunction.takeTraverseClick());

            logger.logInfo("downloaded web pages");

            /***************end 动态链接的抽取******************/


            /*****抽取外链并过滤后缀********/
            JavaPairRDD<Text, Crawldb> baseURLJavaRDD = traverseClickRDD.flatMapToPair(textAnalysis.analysis()).cache();

            JavaPairRDD<Text, Crawldb> result = baseURLJavaRDD.filter(new Function<Tuple2<Text, Crawldb>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Text, Crawldb> textCrawldbTuple2) throws Exception {
                    if (textCrawldbTuple2._2().isFetched()) {
                        logger.logInfo("crawled: " + textCrawldbTuple2._1());
                        return true;
                    } else {
                        return false;
                    }
                }
            }).cache();


            crawledQueue.putRDD(result, jedisPoolUtilsBroadcast, _tidbc);

            TachyonURI tmpout = new TachyonURI(tachyonUrl + "/SparkCrawler/" + tid +starttime+ "/data/" + String.valueOf(round));
            result.values().saveAsObjectFile(tmpout.toString());

            if (!tfs.exist(new TachyonURI(tmpout.getPath() + "/.sync")))
                tfs.createFile(new TachyonURI(tmpout.getPath() + "/.sync"));

            /******************end 抽取外部链接**********************/


            /**********************start 去重********************************/
            JavaPairRDD<Text, Crawldb> newRDD = redisDuplirm.duplicateremove(baseURLJavaRDD, jedisPoolUtilsBroadcast, _tidbc);

            /***********************end 去重***************************************/

            /***********************start 协议，域名，后缀过滤**************************************/
            JavaPairRDD<Text, Crawldb> curBatchCrawlResult =
                    newRDD.filter(protocolFilter.filterCrawldb()).filter(domainFilter.filterFromUrl()).filter(postFixFilter.filterPostFix());
            /***********************end 域名过滤**********************************************/

            /***************************start 深度过滤，并加入待爬取队列***************************************/
            JavaPairRDD<Text, Crawldb> zuizhongRDD = curBatchCrawlResult.filter(preExpansionfilterEnforcer.filterDepth()).cache();
            nextQueue.putNextUrls(zuizhongRDD.filter(domainFilter.filterUrl()), jedisPoolUtilsBroadcast, _tidbc);

            round++;
            crawler_amount +=amount;
            /**************************************每一轮写入监控数据***************************************/
            time = System.currentTimeMillis();
            jedis2.select(3);
            status_map.put("heartbeattime",time);
            status_map.put("taskstatus",1);
            jedis2.hset("sparkcrawl:heartbeat", tid,JSONUtil.object2JacksonString(status_map));

            data_map.put("pass",pass);
            data_map.put("depth",depth);
            data_map.put("crawlCount",crawler_amount);
            jedis2.hset("sparkcrawl:monitor:" + tid, time.toString(), JSONUtil.object2JacksonString(data_map));

            /********************************************************************************************/
            //PaceKeeper.pause();
        }

        time = System.currentTimeMillis();
        jedis2.select(3);
        status_map.put("heartbeattime",time);
        status_map.put("taskstatus",2);
        jedis2.hset("sparkcrawl:heartbeat", tid,JSONUtil.object2JacksonString(status_map));;


        Jedis jedis1 = jedisPoolUtils.getJedisPool().getResource();
        jedis1.select(1);
        jedis1.del(jedis1.keys("sparkcrawler::Crawled::" + tid + "::*").toArray(new String[0]));
        jedis1.select(0);
        jedis1.del("sparkcrawler::ToCrawl::" + tid);
        jedis1.close();
    }

    protected boolean shouldContinue(JedisPoolUtils jedisPoolUtils) throws IOException {
        boolean rs = nextQueue.hasMoreUrls(taskid,jedisPoolUtils);
        logger.logInfo("should continue: " + rs);
        return rs;
    }
}
