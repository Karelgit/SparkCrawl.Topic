package com.gengyun.urlidentifier;

/**
 * Created by hadoop on 2015/11/9.
 */

import java.io.Serializable;

import com.gengyun.metainfo.BaseURL;
import org.apache.spark.api.java.function.FlatMapFunction;


public class RDDURLIdentifier implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = -7147957261469407398L;

    /*public FlatMapFunction<BaseURL, BaseURL> extractUrls() {
        FlatMapFunction<BaseURL, BaseURL> result = new FlatMapFunction<BaseURL, BaseURL>() {
            public Iterable<BaseURL> call(BaseURL base) throws Exception {
                JsoupBasedURLIdentifier iden = new JsoupBasedURLIdentifier();
                return iden.extractUrls(base);
            }
        };
        return result;
    }*/

}