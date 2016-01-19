package com.gengyun.queue;

import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.JedisPoolUtils;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Created by root on 16-1-4.
 */
public class RDDRedisCrawledQue implements Serializable {


    public void putRDD(JavaPairRDD<Text, Crawldb> crawledRDD, final Broadcast<JedisPoolUtils> broadcast, final Broadcast<String> taskid) throws IOException {

        crawledRDD.map(new Function<Tuple2<Text, Crawldb>, String>() {
            @Override
            public String call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                return tuple2._2().getUrl();
            }
        }).repartition(2).foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> datas) throws Exception {
                List<String> list = Lists.newArrayList(datas);
                String[] hashcodes = new String[list.size() * 2];
                if (list.size() != 0) {
                    for (int i = 0; i < list.size(); i++) {
                        hashcodes[2 * i] = "sparkcrawler::Crawled::" + taskid.value() + "::" + String.valueOf(list.get(i).hashCode());
                        hashcodes[2 * i + 1] = String.valueOf(list.get(i).hashCode());
                    }
                }

                Jedis jedis = broadcast.value().getJedisPool().getResource();
                jedis.select(1);
                jedis.mset(hashcodes);
                broadcast.value().getJedisPool().returnResource(jedis);
            }
        });

/*

        crawledRDD.repartition(2);
        crawledRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<Text, Crawldb>>>() {
            @Override
            public void call(Iterator<Tuple2<Text, Crawldb>> tuple2) throws Exception {
                Jedis jedis = new Jedis("108.108.108.15", 6379, 60000);
                jedis.select(1);


                while (tuple2.hasNext()) {
                    Crawldb crawldb = tuple2.next()._2();
                    jedis.set(String.valueOf(crawldb.getUrl().hashCode()), String.valueOf(crawldb.getUrl().hashCode()));
                }

            }
        });*/

    }
}