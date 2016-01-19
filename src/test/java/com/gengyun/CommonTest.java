package com.gengyun;


import com.google.common.collect.Lists;
import redis.clients.jedis.Jedis;
import tachyon.TachyonURI;
import tachyon.client.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.util.*;

/**
 * Created by hadoop on 2015/11/16.
 */
public class CommonTest {
    public static void main(String[] args) throws Exception {
        TachyonURI tmpout = new TachyonURI("tachyon://localhost:19998/SparkCrawler/dasdasdsadas/data/" + String.valueOf("1") + "/" + String.valueOf("0"));
        System.out.println(tmpout.getPath());
    }


    public static void writeFile() throws IOException {
        TachyonFS fs = TachyonFS.get(new TachyonURI("tachyon://localhost:19998"));
        TachyonFile file = fs.getFile(new TachyonURI("/.locked"));
        OutputStream os = file.getOutStream(WriteType.MUST_CACHE);
        os.write("12345454545".getBytes());
        os.close();
    }


    public static void readFile() throws IOException {
        TachyonFS fs = TachyonFS.get(new TachyonURI("tachyon://localhost:19998"));
        TachyonFile file = fs.getFile(new TachyonURI("/.locked"));

        InStream in = file.getInStream(ReadType.CACHE);
        byte[] bytes = new byte[1024];
        in.read(bytes);
        System.out.println(new String(bytes));
        in.close();
    }
}
