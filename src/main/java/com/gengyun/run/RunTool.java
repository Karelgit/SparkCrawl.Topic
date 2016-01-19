package com.gengyun.run;

import org.apache.spark.deploy.SparkSubmit;

/**
 * Created by root on 16-1-8.
 */
public class RunTool {
    public static void main(String[] args) {
        SparkSubmit.main(new String[]{"--master", "local[*]",
                "--class", "com.gengyun.entry.Crawl",
                "--name", "SparkCrawler",
                "./target/SparkWebCrawler-jar-with-dependencies.jar"});
    }
}
