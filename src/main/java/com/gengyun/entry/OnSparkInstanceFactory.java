package com.gengyun.entry;


import com.gengyun.duplirm.RedisDuplirm;
import com.gengyun.flter.PostFixFilter;
import com.gengyun.flter.ProtocolFilter;
import com.gengyun.huanghai.ClickFunction;
import com.gengyun.lsc.analysis.analysis.BaseTemplate;
import com.gengyun.lsc.analysis.analysis.TextAnalysis;
import com.gengyun.queue.RDDRedisCrawledQue;
import com.gengyun.queue.RDDRedisToCrawlQue;
import com.gengyun.urlfilter.RDDPreExpansionFilterEnforcer;
import com.gengyun.urlidentifier.RDDHUURLIdentifier;
import com.gengyun.utils.JedisPoolUtils;
import com.gengyun.webcomm.DownloadRDD;
import com.gengyun.webcomm.HtmlUnitDownload;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Properties;

/**
 * spark爬虫构造工厂
 *
 * @author lhj
 */
public class OnSparkInstanceFactory {
    private static InitSparkConfig SPARK_CONFIG;
    private static OnSparkInstanceFactory singleton;

    public OnSparkInstanceFactory(InitSparkConfig obj,Properties properties) {
        SPARK_CONFIG = obj;
        sparkConf = SPARK_CONFIG.getSparkConf();
        sparkContext = new JavaSparkContext(sparkConf);
        RDDHUURL_IDENTIFIER = new RDDHUURLIdentifier(sparkConf, SPARK_CONFIG.getPostfix());

        POST_FIX_FILTER = new PostFixFilter(SPARK_CONFIG.getPostfix());

        PROTOCOL_FILTER = new ProtocolFilter();
        PROTOCOL_FILTER.setProtocols(SPARK_CONFIG.getProtocols());

        POST_FIX_FILTER = new PostFixFilter(SPARK_CONFIG.getPostfix());

        BASE_TEMPLATES = SPARK_CONFIG.getListTemplate();

        TEXT_ANALYSIS = new TextAnalysis(BASE_TEMPLATES);

        clickFunction = new ClickFunction(SPARK_CONFIG.getRegexList(), SPARK_CONFIG.getRecalldepth());


       // JEDIS_POOL_UTILS=new JedisPoolUtils(properties.getProperty("redis.ip"),properties.getProperty("redis.port"));

    }

    public static OnSparkInstanceFactory getInstance(final InitSparkConfig object,Properties properties) {
        if (singleton == null) {
            synchronized (OnSparkInstanceFactory.class) {
                if (singleton == null) {
                    singleton = new OnSparkInstanceFactory(object,properties);
                }
            }
        }
        return singleton;
    }

    private static SparkConf sparkConf;
    private static JavaSparkContext sparkContext;
    private static ClickFunction clickFunction;

    private static RDDPreExpansionFilterEnforcer rddPreExpansionFilterEnforcer = new RDDPreExpansionFilterEnforcer();

   // private static RDDURLQueue rddURLQueue = new RDDURLQueue();
    //private static JedisPoolUtils JEDIS_POOL_UTILS;//= new JedisPoolUtils();

    private static RDDRedisToCrawlQue REDIS_TO_CRAWL_QUE = new RDDRedisToCrawlQue();

    //private static RDDCrawledQueue RDD_CRAWLED_QUEUE = new RDDCrawledQueue();

    private static RDDRedisCrawledQue RDD_REDIS_CRAWLED_QUE = new RDDRedisCrawledQue();

    private static RDDHUURLIdentifier RDDHUURL_IDENTIFIER;

    private static PostFixFilter POST_FIX_FILTER;

    private static ProtocolFilter PROTOCOL_FILTER;

   // private static DuplicateRemoval DUPLICATE_REMOVAL = new DuplicateRemoval();

    private static RedisDuplirm REDIS_DUPLIRM = new RedisDuplirm();

    private static HtmlUnitDownload HTML_UNIT_DOWNLOAD = new HtmlUnitDownload();

    private static DownloadRDD DOWNLOAD_RDD = new DownloadRDD();


    private static final StructType schema = DataTypes
            .createStructType(new StructField[]{
                    DataTypes.createStructField("url", DataTypes.StringType, true),
                    DataTypes.createStructField("parentUrl", DataTypes.StringType, true),
                    DataTypes.createStructField("time", DataTypes.StringType, true),
                    DataTypes.createStructField("depth", DataTypes.StringType, true),
                    DataTypes.createStructField("fetched", DataTypes.StringType, true), DataTypes.createStructField("statcode", DataTypes.IntegerType, true), DataTypes.createStructField("rootUrl", DataTypes.StringType, true), DataTypes.createStructField("text", DataTypes.StringType, true), DataTypes.createStructField("html", DataTypes.StringType, true), DataTypes.createStructField("title", DataTypes.StringType, true), DataTypes.createStructField("publishtime", DataTypes.LongType, true), DataTypes.createStructField("tag", DataTypes.BooleanType, true)});


    private static List<BaseTemplate> BASE_TEMPLATES;

    private static TextAnalysis TEXT_ANALYSIS;


    public static JavaSparkContext getSparkContext() {
        return sparkContext;
    }




    public static RDDPreExpansionFilterEnforcer getPreExpansionFilterEnforcer() {
        return rddPreExpansionFilterEnforcer;
    }



    public static RDDHUURLIdentifier getRddhuurlIdentifier() {
        return RDDHUURL_IDENTIFIER;
    }

    public static PostFixFilter getPostFixFilter() {
        return POST_FIX_FILTER;
    }

    public static HtmlUnitDownload getHtmlUnitDownload() {
        return HTML_UNIT_DOWNLOAD;
    }

    public static StructType getSchema() {
        return schema;
    }

    public static TextAnalysis getTextAnalysis() {
        return TEXT_ANALYSIS;
    }

    public static ProtocolFilter getProtocolFilter() {
        return PROTOCOL_FILTER;
    }

    /*public static DuplicateRemoval getDuplicateRemoval() {
        return DUPLICATE_REMOVAL;
    }*/

    public static RedisDuplirm getRedisDuplirm() {
        return REDIS_DUPLIRM;
    }

    public static DownloadRDD getDownloadRdd() {
        return DOWNLOAD_RDD;
    }

    /*public static JedisPoolUtils getJedisPoolUtils() {
        return JEDIS_POOL_UTILS;
    }*/

    public static RDDRedisToCrawlQue getRedisToCrawlQue() {
        return REDIS_TO_CRAWL_QUE;
    }

    public static RDDRedisCrawledQue getRddRedisCrawledQue() {
        return RDD_REDIS_CRAWLED_QUE;
    }

    public static ClickFunction getClickFunction() {
        return clickFunction;
    }
}