package com.gengyun;

import redis.clients.jedis.Jedis;

/**
 * Created by root on 16-1-4.
 */
public class RedisTest {
    public static void main(String[] args) {
        Jedis jedis=new Jedis("127.0.0.1",6379);
        jedis.select(1);
        for (String s : jedis.keys("sparkcrawler::Crawled::*")) {
            System.out.println(s);
        }
        
    }
}
