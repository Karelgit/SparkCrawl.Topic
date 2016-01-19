package com.gengyun.flter;

import com.gengyun.metainfo.Crawldb;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.net.URL;
import java.util.HashSet;

/**
 * Created by lhj on 15-12-24.
 */
public class DomainFilter implements Serializable {

    private HashSet<String> domains;


    public DomainFilter(HashSet<String> domains) {
        this.domains = domains;

    }


    public Function<Tuple2<Text, Crawldb>, Boolean> filterFromUrl() {
        Function<Tuple2<Text, Crawldb>, Boolean> result = new Function<Tuple2<Text, Crawldb>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                URL parentURL = new URL(tuple2._2().getFromUrl());
                String domain = parentURL.getHost();
                if (domain.startsWith("www")) {
                    domain = domain.replaceFirst("www.", "");
                }
                if (domains.contains(domain)) {
                    return true;
                } else {
                    return false;
                }
            }
        };

        return result;
    }

    public Function<Tuple2<Text, Crawldb>, Boolean> filterUrl() {
        Function<Tuple2<Text, Crawldb>, Boolean> result = new Function<Tuple2<Text, Crawldb>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                URL parentURL = new URL(tuple2._2().getUrl());
                String domain = parentURL.getHost();
                if (domain.startsWith("www")) {
                    domain = domain.replaceFirst("www.", "");
                }
                if (domains.contains(domain)) {
                    return true;
                } else {
                    return false;
                }
            }
        };

        return result;
    }

}
