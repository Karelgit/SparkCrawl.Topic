package com.gengyun.huanghai.test;

import com.gengyun.utils.ReadFromTachyon;
import org.codehaus.jettison.json.JSONObject;
import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;

import java.io.*;
import java.util.Properties;

/**
 * Created by root on 16-1-25.
 */
public class Read {
    public static void main(String[] args) throws IOException {
        Properties properties=new Properties();
        InputStream in=new BufferedInputStream(new FileInputStream("/opt/IDEAProject/SparkWebCrawler/src/main/resources/db.properties"));
        properties.load(in);
        String tachyonUrl=properties.getProperty("tachyon.url");
        String inputFilePath = "/SparkCrawler/SparkCrawler20160120094121/protocol/protocol.txt";
        TachyonFS fs = TachyonFS.get(new TachyonURI(tachyonUrl));
        String str = ReadFromTachyon.getfilecontent(fs, inputFilePath);
        String s[] = str.split("\n");
        System.out.println(s.length);
        for (String s1 : s) {
            System.out.println(s1.trim());
        }
    }
}
