package com.gengyun;

import com.gengyun.metainfo.CrawlDatum;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException, Exception {

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("APP").set("spark.tachyonStore.url", "tachyon://localhost:19998/");
        sparkConf.registerKryoClasses(new Class<?>[]{
                Class.forName("org.apache.hadoop.io.Text"),
                Class.forName("com.gengyun.metainfo.CrawlDatum")
        });
        TachyonFS fs = TachyonFS.get(new TachyonURI("tachyon://localhost:19998"));


        if (fs.exist(new TachyonURI("/seqTest"))) {
            fs.delete(new TachyonURI("/seqTest"), true);
        }


        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        jsc.hadoopConfiguration().set("fs.tachyon.impl", "tachyon.hadoop.TFS");


        CrawlDatum crawlDatum = new CrawlDatum();


        JavaPairRDD<Text, CrawlDatum> originRDD = jsc.parallelizePairs(Arrays.asList(new Tuple2<Text, CrawlDatum>(new Text("http://www.baidu.com"), crawlDatum),
                new Tuple2<Text, CrawlDatum>(new Text("http://www.taobao.com"), crawlDatum), new Tuple2<Text, CrawlDatum>(new Text("123"), crawlDatum)));
        originRDD.saveAsHadoopFile("tachyon://localhost:19998/seqTest", Text.class, CrawlDatum.class, SequenceFileOutputFormat.class);
        JavaPairRDD<Text, CrawlDatum> originRDD1 = jsc.sequenceFile("tachyon://localhost:19998/seqTest", Text.class, CrawlDatum.class);

        JavaPairRDD<Text, CrawlDatum> top1 = jsc.parallelizePairs(originRDD.take(1));


        JavaPairRDD<Text,CrawlDatum> topn_1= originRDD1.union(top1).flatMap(new FlatMapFunction<Tuple2<Text, CrawlDatum>, Tuple2<Text, CrawlDatum>>() {
            @Override
            public Iterable<Tuple2<Text, CrawlDatum>> call(Tuple2<Text, CrawlDatum> tuple2) throws Exception {
                ArrayList<Tuple2<Text, CrawlDatum>> arrayList = new ArrayList<Tuple2<Text, CrawlDatum>>();
                CrawlDatum crawlDatum = new CrawlDatum();
                crawlDatum.setUrl("http://www.baidu.com");
                crawlDatum.setPublishtime(System.currentTimeMillis());
                crawlDatum.setCrawltime(System.currentTimeMillis());
                crawlDatum.setDepthfromSeed(tuple2._2().getDepthfromSeed() + 1);

                arrayList.add(new Tuple2<Text, CrawlDatum>(new Text("123"), crawlDatum));
                return arrayList;
            }
        }).mapToPair(new PairFunction<Tuple2<Text, CrawlDatum>, Text, CrawlDatum>() {
            @Override
            public Tuple2<Text, CrawlDatum> call(Tuple2<Text, CrawlDatum> tuple2) throws Exception {
                return tuple2;
            }
        });
        for (Tuple2<Text, CrawlDatum> tuple2 : topn_1.collect()) {
            System.out.println(tuple2._2().getUrl().toString() + "\t" + tuple2._2().getDepthfromSeed());
        }

      /*  JavaRDD<String>  oldRDD=jsc.textFile(old);
        JavaRDD<String> newRDD = rdd.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {

                return s + String.valueOf(System.currentTimeMillis());
            }
        });*/

/*


        TachyonURI output = new TachyonURI("tachyon://108.108.108.15:19998/iteblog/" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        rdd.union(oldRDD).saveAsTextFile(output.toString());

        TachyonURI oldURI = new TachyonURI("/iteblog/old");
        TachyonURI currentURI = new TachyonURI("/iteblog/current");

        if (fs.exist(currentURI)) {
            if (fs.exist(oldURI)) fs.delete(oldURI, true);
            fs.rename(currentURI, oldURI);
        }

    fs.rename(new TachyonURI(output.getPath()), new TachyonURI("/iteblog/current"));
*/

        // newRDD.saveAsTextFile("tachyon://108.108.108.15:19998/iteblog");

        /*JavaRDD<String> splitrdd = rdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        if (fs.exist(new TachyonURI("/iteblog"))) {
            fs.delete(new TachyonURI("/iteblog"), true);
        }

        splitrdd.saveAsTextFile("tachyon://108.108.108.15:19998/iteblog/current");*/


    }
}
