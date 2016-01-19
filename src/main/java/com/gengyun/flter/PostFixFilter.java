package com.gengyun.flter;

import com.gengyun.metainfo.CrawlDatum;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;

/**
 * Created by root on 15-12-16.
 */
public class PostFixFilter implements Serializable {
    private HashSet<String> postfix;

    public PostFixFilter(HashSet<String> postfix) {
        this.postfix = postfix;

    }

    public Function<Tuple2<Text, CrawlDatum>, Boolean> filter() {
        Function<Tuple2<Text, CrawlDatum>, Boolean> result = new Function<Tuple2<Text, CrawlDatum>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Text, CrawlDatum> tuple2) throws Exception {
                String urlStr = tuple2._1().toString();
                if (postfix.contains(urlStr.substring(urlStr.lastIndexOf(".") + 1))) {
                    return false;
                } else
                    return true;
            }
        };
        return result;
    }

}
