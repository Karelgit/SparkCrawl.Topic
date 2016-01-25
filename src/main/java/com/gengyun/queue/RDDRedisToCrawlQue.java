package com.gengyun.queue;

import com.gengyun.entry.OnSparkInstanceFactory;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.*;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by root on 16-1-4.
 */
public class RDDRedisToCrawlQue implements Serializable {
    private transient static LogManager logger = new LogManager(RDDURLQueue.class);


    private static final int batchsize = 100;


    public JavaPairRDD<Text, Crawldb> nextBatch(Broadcast<JedisPoolUtils> jedisPoolUtilsBroadcast, Broadcast<String> taskidBroadcast) throws IOException {
        String taskid = taskidBroadcast.value();
        JavaSparkContext jsc = OnSparkInstanceFactory.getSparkContext();
        List<String> redisdata = new ArrayList<>();
        Jedis jedis = jedisPoolUtilsBroadcast.value().getJedisPool().getResource();

        for (int i = 0; i < batchsize; i++) {
            String dat = jedis.lpop("sparkcrawler::ToCrawl::" + taskid);
            if (dat != null) {
                redisdata.add(dat);
            }
        }
        jedisPoolUtilsBroadcast.value().getJedisPool().returnResource(jedis);
        JavaPairRDD<Text, Crawldb> result = jsc.parallelize(redisdata).mapToPair(new PairFunction<String, Text, Crawldb>() {
            @Override
            public Tuple2<Text, Crawldb> call(String s) throws Exception {
                Crawldb crawldb = (Crawldb) JSONUtil.jackson2Object(s, Crawldb.class);

                return new Tuple2<Text, Crawldb>(new Text(crawldb.getUrl()), crawldb);
            }
        });

        return result;
    }

    public boolean hasMoreUrls(String taskid,JedisPoolUtils jedisPoolUtils) {

        Jedis jedis = jedisPoolUtils.getJedisPool().getResource();
        try {
            return jedis.llen("sparkcrawler::ToCrawl::"+taskid) != 0;
        } finally {
            jedisPoolUtils.getJedisPool().returnResource(jedis);
        }

    }

    public void putNextUrls(JavaPairRDD<Text, Crawldb> nextUrls, final Broadcast<JedisPoolUtils> jedisPoolUtilsBroadcast, final Broadcast<String> taskid) throws IOException {

        JavaRDD<String> data = nextUrls.map(new Function<Tuple2<Text, Crawldb>, String>() {
            @Override
            public String call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                return JSONUtil.object2JacksonString(tuple2._2());
            }
        });
        data.repartition(2);

        data.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> bytes) throws Exception {
                ArrayList<String> data = Lists.newArrayList(bytes);

                if (data.size() != 0) {
                    Jedis jedis = jedisPoolUtilsBroadcast.value().getJedisPool().getResource();
                    jedis.select(0);
                    jedis.rpush("sparkcrawler::ToCrawl::" + taskid.value(), data.toArray(new String[0]));
                    jedisPoolUtilsBroadcast.value().getJedisPool().returnResource(jedis);
                }

            }
        });


    }

}
