package com.gengyun;

import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.JSONUtil;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by hadoop on 2015/11/16.
 */
public class MyTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        SparkConf sparkConf = new SparkConf()/*.setMaster("local[*]")*/.setAppName("MyTest");
        sparkConf.registerKryoClasses(new Class<?>[]{
                Class.forName("org.apache.hadoop.io.IntWritable"),
                Class.forName("org.apache.hadoop.io.Text")
        });

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        Crawldb crawldb=new Crawldb();
        crawldb.setUrl("http://www.baidu.com");
        crawldb.setFromUrl("http://www.baidu.com");
        crawldb.setRootUrl("http://www.baidu.com");

        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList(JSONUtil.object2JacksonString(crawldb), JSONUtil.object2JacksonString(crawldb), JSONUtil.object2JacksonString(crawldb), JSONUtil.object2JacksonString(crawldb), JSONUtil.object2JacksonString(crawldb)));
        rdd.repartition(2);

        rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> arr) throws Exception {
                Jedis jedis = new Jedis("108.108.108.15", 6379);

                while (arr.hasNext()) {
                    String item = arr.next();
                    jedis.rpush("sparkredis", item);
                }

            }
        });


        Jedis jedis = new Jedis("108.108.108.15", 6379);
        List<String> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String dat = jedis.lpop("sparkredis");
            if (dat != null) {
                data.add(dat);
            }

        }


        JavaPairRDD<Text,Crawldb> pairRDD=jsc.parallelize(data).mapToPair(new PairFunction<String, Text, Crawldb>() {
            @Override
            public Tuple2<Text, Crawldb> call(String s) throws Exception {
                Crawldb crawldb=(Crawldb) JSONUtil.jackson2Object(s,Crawldb.class);

                return new Tuple2<Text, Crawldb>(new Text(crawldb.getUrl()),crawldb);
            }
        });

        for (Tuple2<Text, Crawldb> tuple2 : pairRDD.collect()) {
            System.out.println(tuple2._1().toString());
        }


        /*RedisKeysRDD redisKeysRDD = sc.fromRedisKeyPattern(new Tuple2<String, Object>("127.0.0.1", 6379), "sparkredis", 1);

        JavaRDD<String> vals = redisKeysRDD.getList().toJavaRDD();

        for (String s : vals.collect()) {
            System.out.println(s);
        }*/

/*        JavaPairRDD<String, String> files = jsc.wholeTextFiles("tachyon://localhost:19998/seenUrls/current," +
                "tachyon://localhost:19998/seenUrlsTest1");


        JavaRDD<String> splitedRDD = files.values().flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\n"));
            }
        }).mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {
                return new Tuple2<String, Long>(s, 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        }).filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return stringLongTuple2._2() == 1L;
            }
        }).keys();

        System.out.println(splitedRDD.collect());*/


        jsc.stop();

    }
}
