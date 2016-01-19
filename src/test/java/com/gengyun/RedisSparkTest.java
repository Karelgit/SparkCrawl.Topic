package com.gengyun;

import com.gengyun.utils.JSONUtil;
import com.gengyun.utils.JedisPoolUtils;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by root on 16-1-4.
 */
public class RedisSparkTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("RedisSparkTest");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);


        final Broadcast<JedisPoolUtils> jedisPoolBroadcast= jsc.broadcast(new JedisPoolUtils());
        JavaRDD<Person> rdd = jsc.parallelize(Arrays.asList(new Person(1, "a"), new Person(2, "b"), new Person(3, "c")), 1);


         JavaRDD<String> data= rdd.map(new Function<Person, String>() {
            @Override
            public String call(Person person) throws Exception {
                return JSONUtil.object2JacksonString(person);
            }
        }).repartition(2);
        data.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> arr) throws Exception {


                Jedis jedis = jedisPoolBroadcast.value().getJedisPool().getResource(); //new Jedis("108.108.108.15", 6379,60000);

                jedis.rpush("test", Lists.newArrayList(arr).toArray(new String[0]));

                jedisPoolBroadcast.value().getJedisPool().returnResourceObject(jedis);
            }
        });


    }
}
