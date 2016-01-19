package com.gengyun.webcomm;

import com.gengyun.entry.InstanceFactory;
import com.gengyun.metainfo.BaseURL;
import com.gengyun.metainfo.CrawlDatum;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by hadoop on 2015/11/9.
 */
public class RDDFunctionWebCommManager implements Serializable {


    /**
     *
     */
    private static final long serialVersionUID = -4566694261803813944L;

    public static Function<BaseURL, BaseURL> downloadPageContent() {
        Function<BaseURL, BaseURL> result = new Function<BaseURL, BaseURL>() {
            public BaseURL call(BaseURL base) {
                //jsoup获取连接
                //base.downloadPageContent(InstanceFactory.getWebCommManager());
                base.downloadPageContent(InstanceFactory.getHUWebCommManager());
                return base;
            }
        };
        return result;
    }

    public static PairFunction<Tuple2<Text, CrawlDatum>, Text, CrawlDatum> downloadPage() {
        PairFunction<Tuple2<Text, CrawlDatum>, Text, CrawlDatum> result = new PairFunction<Tuple2<Text, CrawlDatum>, Text, CrawlDatum>() {
            @Override
            public Tuple2<Text, CrawlDatum> call(Tuple2<Text, CrawlDatum> t) throws Exception {

                t._2().downloadPageContent(InstanceFactory.getHUWebCommManager());




                return t;

            }
        };
        return result;
    }


    public static Function<BaseURL, Boolean> filterEmptyUrls() {
        Function<BaseURL, Boolean> result = new Function<BaseURL, Boolean>() {
            public Boolean call(BaseURL base) {
                return base.isValid();
            }
        };
        return result;
    }

    public static Function<Tuple2<Text, CrawlDatum>, Boolean> filterEmpty() {
        Function<Tuple2<Text, CrawlDatum>, Boolean> result = new Function<Tuple2<Text, CrawlDatum>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Text, CrawlDatum> tuple2) throws Exception {
                if (tuple2._2().isValid()) {
                    tuple2._2().setHtml(tuple2._2().getWebPage().getPageHtml());
                    return true;
                } else {
                    return false;
                }
            }
        };

        return result;
    }

}