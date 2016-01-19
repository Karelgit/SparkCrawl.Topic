package com.gengyun.entry;

import com.gengyun.crawler.OnSparkWorkflowManager;
import com.gengyun.metainfo.Crawldb;
import org.apache.hadoop.io.Text;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

/**
 * 主程序入口
 * Created by hadoop on 2015/11/9.
 */

public class Crawl {

    public static void kick(int depth, int pass, String tid, String starttime, String seedpath, String type, String mode, int recalldepth, String templatesDir, String clickregexDir) throws IOException {
        InitSparkConfig sparkConfig = new InitSparkConfig(tid, mode, recalldepth, templatesDir, clickregexDir);

        OnSparkInstanceFactory.getInstance(sparkConfig);
        ConfigLoader configLoader = new ConfigLoader();
        List<Tuple2<Text, Crawldb>> seeds = configLoader.load(depth, tid, starttime, pass, seedpath, type);

        OnSparkWorkflowManager workflow = new OnSparkWorkflowManager(tid);
        workflow.crawl(seeds, tid, starttime, pass);
    }

    public static void main(String[] args) {
        if (args.length < 20) {
            System.out.println("Usage:\n" +
                    "\t  -depth <退出深度>\n" +
                    "\t  -pass <遍数>\n" +
                    "\t  -tid <实例ID>\n" +
                    "\t  -starttime <实例启动时间>" +
                    "\t  -seedpath <种子路径>\n" +
                    "\t  -type <实例类型>\n" +
                    "\t  -recalldepth <回溯点击层数>\n" +
                    "\t  -templateDir <模板目录>\n" +
                    "\t  -clickregexDir <点击正则表达式目录>\n" +
                    "\t  -postregexDir <后缀过滤目录>\n" +
                    "\t  -mode <local/cluster>");
            System.exit(-1);
        }

        try {
            int depth = 1;
            int pass = 1;//遍数
            String tid = "SparkCrawler";
            String starttime = "";
            String seedpath = "/opt/IdeaProjects/gengyun/SparkWebCrawler/data/inputConfig.json";
            String type = "topic";
            String mode = "local[*]";
            int recalldepth = 3;
            String templateDir = "/SparkCrawler/templates";
            String clickregexDir = "/SparkCrawler/clickregex";
            String postregexDir = "/SparkCrawler/postregexDir";

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
                } else if ("-type".equals(args[i])) {
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
                } else if ("-postregexDir".equals(args[i + 1])) {
                    postregexDir = args[i + 1];
                    i++;
                }

            }


            kick(depth, pass, tid, starttime, seedpath, type, mode, recalldepth, templateDir, clickregexDir);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
