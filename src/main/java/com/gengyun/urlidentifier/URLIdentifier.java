package com.gengyun.urlidentifier;

import com.gengyun.metainfo.BaseURL;
import com.gengyun.metainfo.Crawldb;
import org.apache.hadoop.io.Text;
import scala.Tuple2;

import java.util.List;

/**
 * Created by hadoop on 2015/11/9.
 */
public interface URLIdentifier {
    public List<BaseURL> extractUrls(BaseURL url);

    public List<Tuple2<Text, Crawldb>> extractCrawlDatumUrls(Tuple2<Text, Crawldb> base);
}
