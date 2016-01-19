import com.gengyun.crawler.MyUrl;
import com.gengyun.entry.OnSparkInstanceFactory;
import com.gengyun.metainfo.CrawlDatum;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.PropertyHelper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by root on 15-12-11.
 */
public class ReadTachyon {
    public static void main(String[] args) throws ClassNotFoundException {
        SparkConf sparkConf = new SparkConf().setAppName("APP").setMaster("local[*]").set("spark.externalBlockStore.url", "tachyon://localhost:19998").set("fs.tachyon-ft.impl", "tachyon.hadoop.TFSFT").set("spark.executor.memory", "6g").set("spark.driver.memory", "3g").set("spark.default.parallelism", "10").registerKryoClasses(new Class<?>[]{
                Class.forName("org.apache.hadoop.io.Text"),
                Class.forName("com.gengyun.metainfo.CrawlDatum"),
                Class.forName("com.gengyun.metainfo.Crawldb")
        });


        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "root");
        properties.setProperty("useUnicode", "true");
        properties.setProperty("characterEncoding", "utf8");
        properties.setProperty("autoReconnect", "true");
        properties.setProperty("rewriteBatchedStatements", "true");
        PropertyHelper helper = new PropertyHelper("db");
        String dburl = helper.getValue("db.url");
        String table = helper.getValue("db.table");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<Crawldb> result = jsc.objectFile("tachyon://localhost:19998/Crawled/old");

        JavaPairRDD<Text, Crawldb> reducers = result.mapToPair(new PairFunction<Crawldb, Text, Crawldb>() {
            @Override
            public Tuple2<Text, Crawldb> call(Crawldb crawldb) throws Exception {
                return new Tuple2<Text, Crawldb>(new Text(crawldb.getUrl()), crawldb);
            }
        });


        for (Tuple2<Text, Crawldb> crawldbTuple2 : reducers.collect()) {
            System.out.println(crawldbTuple2._1());
        }


    }
}
