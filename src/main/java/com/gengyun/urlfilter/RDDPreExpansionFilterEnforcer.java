package com.gengyun.urlfilter;

/**
 * Created by hadoop on 2015/11/9.
 */

import java.io.IOException;
import java.io.Serializable;

import com.gengyun.metainfo.BaseURL;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.LogManager;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;


public class RDDPreExpansionFilterEnforcer extends FilterEnforcer implements Serializable {
    private static transient LogManager logger = new LogManager(RDDPreExpansionFilterEnforcer.class);

    /**
     *
     */
    private static final long serialVersionUID = -2356251362384822272L;

    public Function<BaseURL, Boolean> filter() {
        // RDDPreExpansionFilterEnforcer enf = this;
        Function<BaseURL, Boolean> result = new Function<BaseURL, Boolean>() {
            public Boolean call(BaseURL base) {
                try {
                    boolean result = applyFilters(base);
                    return result;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        return result;
    }

    public Function<Tuple2<Text, Crawldb>, Boolean> filterDepth() {
        Function<Tuple2<Text, Crawldb>, Boolean> result = new Function<Tuple2<Text, Crawldb>, Boolean>() {
            public Boolean call(Tuple2<Text, Crawldb> base) {
                boolean result = false;
                try {
                    result = applyDepthFilters(base);

                } catch (IOException e) {

                } finally {
                    return result;
                }

            }
        };
        return result;
    }

}
