package com.gengyun.entry;

import com.gengyun.crawler.OnSparkWorkflowManager;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.JedisPoolUtils;
import org.apache.hadoop.io.Text;
import org.glassfish.grizzly.utils.BufferInputStream;
import scala.Tuple2;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * 主程序入口
 * Created by hadoop on 2015/11/9.
 */

public class Crawl {

    public static void kick(int depth, int pass, String tid, String starttime, String seedpath, String protocolDir, String postregexDir, String type, String mode, int recalldepth, String templatesDir, String clickregexDir,String configpath) throws IOException {
        Properties properties=new Properties();
        InputStream in=new BufferedInputStream(new FileInputStream(configpath));
        properties.load(in);
        String tachyonUrl=properties.getProperty("tachyon.url");


        InitSparkConfig sparkConfig = new InitSparkConfig(tid, mode, recalldepth, templatesDir, clickregexDir, protocolDir, postregexDir,tachyonUrl);
        OnSparkInstanceFactory.getInstance(sparkConfig,properties);

        ConfigLoader configLoader = new ConfigLoader();
        List<Tuple2<Text, Crawldb>> seeds = configLoader.load(depth, tid, starttime, pass, seedpath, type,tachyonUrl);

        OnSparkWorkflowManager workflow = new OnSparkWorkflowManager(tid,tachyonUrl);
        workflow.crawl(seeds, tid, starttime, pass,properties.getProperty("redis.ip"),properties.getProperty("redis.port"));
    }

    public static void main(String[] args) {
        if (args.length < 26) {
            System.out.println("Usage:\n" +
                    "\t  -depth <退出深度>\n" +
                    "\t  -pass <遍数>\n" +
                    "\t  -tid <实例ID>\n" +
                    "\t  -starttime <实例启动时间>" +
                    "\t  -seedpath <种子路径>\n" +
                    "\t  -protocolDir <协议过滤目录>\n" +
                    "\t  -type <实例类型>\n" +
                    "\t  -recalldepth <回溯点击层数>\n" +
                    "\t  -templateDir <模板目录>\n" +
                    "\t  -clickregexDir <点击正则表达式目录>\n" +
                    "\t  -postregexDir <后缀过滤目录>\n" +
                    "\t  -mode <local/cluster>\n"+
                    "\t -configpath <配置文件路径>");
            System.exit(-1);
        }

        try {
            int depth = 1;
            int pass = 1;//遍数
            String tid = "SparkCrawler";
            String starttime = "";
            String seedpath = "/opt/IdeaProjects/gengyun/SparkWebCrawler/data/inputConfig.json";
            String protocolDir = "";
            String type = "topic";
            String mode = "local[*]";
            int recalldepth = 3;
            String templateDir = "/SparkCrawler/templates";
            String clickregexDir = "/SparkCrawler/clickregex";
            String postregexDir = "";
            String configpath="";

            for (int i = 0; i < args.length; i++) {
                if ("-depth".equals(args[i])) {
                    depth = Integer.valueOf(args[i + 1]);
                    i++;
                } else if ("-pass".equals(args[i])) {
                    pass = Integer.valueOf(args[i + 1]);
                    i++;
                } else if ("-tid".equals(args[i])) {
                    tid = args[i + 1];
                    i++;
                } else if ("-starttime".equals(args[i])) {
                    starttime = args[i + 1];
                    i++;
                } else if ("-seedpath".equals(args[i])) {
                    seedpath = args[i + 1];
                    i++;
                } else if ("-protocolDir".equals(args[i])) {
                    protocolDir = args[i + 1];
                    i++;
                }else if ("-type".equals(args[i])) {
                    type = args[i + 1];
                    i++;
                } else if ("-mode".equals(args[i])) {
                    mode = args[i + 1];
                    i++;
                } else if ("-recalldepth".equals(args[i])) {
                    recalldepth = Integer.valueOf(args[i + 1]);
                    i++;
                } else if ("-templateDir".equals(args[i])) {
                    templateDir = args[i + 1];
                    i++;
                } else if ("-clickregexDir".equals(args[i])) {
                    clickregexDir = args[i + 1];
                    i++;
                } else if ("-postregexDir".equals(args[i])) {
                    postregexDir = args[i + 1];
                    i++;
                }else if("-configpath".equals(args[i])){
                    configpath=args[i+1];
                    i++;
                }

            }

            kick(depth, pass, tid, starttime, seedpath, protocolDir, postregexDir, type, mode, recalldepth, templateDir, clickregexDir,configpath);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
