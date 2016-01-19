#!/bin/sh
#!/bin/bash

starttime=$(date +%s)

/opt/spark-1.4.1-bin-hadoop2.6/bin/spark-submit \
 --master spark://108.108.108.15:18888 \
 --class com.gengyun.entry.Crawl \
 --executor-memory 10G \
 --total-executor-cores 10 \
 --driver-memory 5g \
 --name SparkWebCralwer \
 --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
 /opt/SparkWebCrawler-jar-with-dependencies.jar \
 1000


endtime=$(date +%s)
timeconsume=$((endtime-starttime))
echo "耗时："
echo "$timeconsume秒"
