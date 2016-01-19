package com.gengyun.duplirm;

import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.JedisPoolUtils;
import com.gengyun.utils.LogManager;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by root on 16-1-7.
 */
public class RedisDuplirm implements Serializable {
    private static transient LogManager logger = new LogManager(RedisDuplirm.class);


    public JavaPairRDD<Text, Crawldb> duplicateremove(JavaPairRDD<Text, Crawldb> current, final Broadcast<JedisPoolUtils> jedisPoolUtilsBroadcast, final Broadcast<String> taskid) {
        JavaPairRDD<Text, Crawldb> newRDD = current.filter(new Function<Tuple2<Text, Crawldb>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                return !tuple2._2().isFetched();
            }
        }).mapToPair(new PairFunction<Tuple2<Text, Crawldb>, Text, Crawldb>() {
            @Override
            public Tuple2<Text, Crawldb> call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                return new Tuple2<Text, Crawldb>(new Text(tuple2._2().getUrl() + "|" + tuple2._2().getFromUrl()), tuple2._2());
            }
        }).reduceByKey(new Function2<Crawldb, Crawldb, Crawldb>() {
            @Override
            public Crawldb call(Crawldb crawldb, Crawldb crawldb2) throws Exception {
                return crawldb;
            }
        }).mapToPair(new PairFunction<Tuple2<Text, Crawldb>, Text, Crawldb>() {
            @Override
            public Tuple2<Text, Crawldb> call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                return new Tuple2<Text, Crawldb>(new Text(tuple2._1().toString().split("\\|")[0]), tuple2._2());
            }
        }).filter(new Function<Tuple2<Text, Crawldb>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                Jedis jedis = jedisPoolUtilsBroadcast.value().getJedisPool().getResource();
                try {
                    jedis.select(1);
                    if (jedis.exists("sparkcrawler::Crawled::" + taskid.value() + "::" + String.valueOf(tuple2._2().getUrl().hashCode()))) {
                        logger.logInfo(tuple2._1() + "\t" + "\t" + tuple2._2().getFromUrl() + "\thas seen before.");
                        return false;
                    } else {
                        logger.logInfo(tuple2._1() + "\t" + tuple2._2().getFromUrl() + "\thas not been seen.");
                        return true;
                    }
                } finally {
                    jedisPoolUtilsBroadcast.value().getJedisPool().returnResource(jedis);
                }

            }
        });

        return newRDD;
    }

}
