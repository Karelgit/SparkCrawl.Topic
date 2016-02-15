package com.gengyun.urlfilter;

/**
 * Created by hadoop on 2015/11/9.
 */

import com.gengyun.metainfo.BaseURL;
import com.gengyun.metainfo.Crawldb;
import org.apache.hadoop.io.Text;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;


public interface Filter extends Serializable {
    boolean filter(BaseURL url) throws IOException;

    boolean filterDepth(Tuple2<Text,Crawldb> base) throws  IOException;
}
