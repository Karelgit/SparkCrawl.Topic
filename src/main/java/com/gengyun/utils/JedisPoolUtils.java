package com.gengyun.utils;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;

/**
 * Created by root on 15-12-30.
 */
public class JedisPoolUtils implements Serializable {
    private static JedisPool pool;

    public JedisPoolUtils() {
        makepool();
    }

    public static void makepool() {
        if (pool == null) {
            PropertyHelper helper = new PropertyHelper("db");
            JedisPoolConfig conf = new JedisPoolConfig();
            conf.setMaxTotal(1000);
            conf.setMaxWaitMillis(60000L);
            pool = new JedisPool(conf, helper.getValue("redis.ip"), Integer.valueOf(helper.getValue("redis.port")));
        }
    }

    public JedisPool getJedisPool() {
        return pool;
    }
}
