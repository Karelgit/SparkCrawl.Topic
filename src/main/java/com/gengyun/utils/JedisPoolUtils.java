package com.gengyun.utils;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.*;
import java.util.Properties;

/**
 * Created by root on 15-12-30.
 */
public class JedisPoolUtils implements Serializable {
    private static JedisPool pool;

    public JedisPoolUtils() throws FileNotFoundException,IOException{
        makepool();
    }

    public static void makepool() throws FileNotFoundException,IOException {
        if (pool == null) {
            //PropertyHelper helper = new PropertyHelper("db");
            Properties properties=new Properties();
            InputStream in=new BufferedInputStream(new FileInputStream("/opt/topiconfig/sparkcrawl.cfg"));
            properties.load(in);

            JedisPoolConfig conf = new JedisPoolConfig();
            conf.setMaxTotal(1000);
            conf.setMaxWaitMillis(60000L);
            pool = new JedisPool(conf, properties.getProperty("redis.ip"), Integer.valueOf(properties.getProperty("redis.port")));
        }
    }

    public  JedisPool getJedisPool() {
        return pool;
    }
}
