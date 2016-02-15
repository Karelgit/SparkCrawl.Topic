package com.gengyun.huanghai;

import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gengyun.metainfo.Crawldb;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.gengyun.huanghai.recursive_width.*;

/**动态链接的抽取
 * Created by Administrator on 2015/12/16.
 */
public class ClickFunction implements Serializable {
    private List<String> regexList;
    private int recallDepth;

    public ClickFunction(List<String> regexList, int recallDepth) {
        this.regexList = regexList;
        this.recallDepth = recallDepth;
    }

    public Function<Tuple3<Text, Crawldb, HtmlPage>, Tuple4<Text, Crawldb, HtmlPage, Params>> initParams() {
        Function<Tuple3<Text, Crawldb, HtmlPage>, Tuple4<Text, Crawldb, HtmlPage,Params>> result = new Function<Tuple3<Text, Crawldb, HtmlPage>, Tuple4<Text, Crawldb, HtmlPage, Params>>() {
            @Override
            public Tuple4<Text, Crawldb, HtmlPage, Params> call(Tuple3<Text, Crawldb, HtmlPage> textCrawldbHtmlPageTuple3) throws Exception {
                if(columnPageJudge(textCrawldbHtmlPageTuple3._3(),regexList))   {
                    String url = textCrawldbHtmlPageTuple3._2().getUrl();
                    Params params = getDynamicPages(url, regexList,recallDepth);
                    Crawldb base = textCrawldbHtmlPageTuple3._2();
                    return new Tuple4<Text, Crawldb, HtmlPage,Params>(new Text(url),base,textCrawldbHtmlPageTuple3._3(),params);
                }else   {
                    String url = textCrawldbHtmlPageTuple3._2().getUrl();
                    Crawldb base = textCrawldbHtmlPageTuple3._2();
                    return new Tuple4<Text, Crawldb, HtmlPage,Params>(new Text(url),base,textCrawldbHtmlPageTuple3._3(),null);
                }

            }

        };
        return result;
    }

    public PairFlatMapFunction<Tuple4<Text, Crawldb, HtmlPage, Params>, Text, Crawldb> takeTraverseClick()  {
        PairFlatMapFunction<Tuple4<Text, Crawldb, HtmlPage, Params>, Text, Crawldb> result = new PairFlatMapFunction<Tuple4<Text, Crawldb, HtmlPage, Params>, Text, Crawldb>() {
            @Override
            public Iterable<Tuple2<Text, Crawldb>> call(Tuple4<Text, Crawldb, HtmlPage, Params> textCrawldbHtmlPageParamsTuple4) throws Exception {
                if(textCrawldbHtmlPageParamsTuple4._4() !=null) {
                    String url = textCrawldbHtmlPageParamsTuple4._2().getUrl();
                    HtmlPage upperPage = textCrawldbHtmlPageParamsTuple4._3();
                    Params params = textCrawldbHtmlPageParamsTuple4._4();
                    params.setFirstEntry(true);
                    List<String> pageList = traverse_width(params.getK(),params.getLayer(), url, regexList, params.getTagListBlock(), params.getWebClient(), upperPage, params.isFirstEntry());
                    List<Tuple2<Text, Crawldb>> tuple2List = new ArrayList<Tuple2<Text, Crawldb>>();
                    for (String page : pageList) {
                        Crawldb base = new Crawldb();
                        base.setUrl(url);
                        base.setTid(textCrawldbHtmlPageParamsTuple4._2().getTid());
                        base.setPassed(textCrawldbHtmlPageParamsTuple4._2().getPassed());
                        base.setType(textCrawldbHtmlPageParamsTuple4._2().getType());
                        base.setStarttime(textCrawldbHtmlPageParamsTuple4._2().getStarttime());
                        base.setFromUrl(textCrawldbHtmlPageParamsTuple4._2().getFromUrl());
                        base.setRootUrl(textCrawldbHtmlPageParamsTuple4._2().getRootUrl());
                        base.setDepthfromSeed(textCrawldbHtmlPageParamsTuple4._2().getDepthfromSeed());
                        base.setHtml(page.replaceFirst("<\\?xml version=\"1.0\" encoding=\"(.+)\"\\?>", "<!DOCTYPE html>"));
                        base.setStatcode(textCrawldbHtmlPageParamsTuple4._2().getStatcode());
                        Tuple2 tuple2 = new Tuple2<Text,Crawldb>(new Text(url), base);
                        tuple2List.add(tuple2);
                    }
                    return tuple2List;
                }else {
                    /*String url = textCrawldbHtmlPageParamsTuple4._2().getUrl();
                    HtmlPage upperPage = textCrawldbHtmlPageParamsTuple4._3();
                    List<Tuple2<Text, Crawldb>> tuple2List = new ArrayList<Tuple2<Text, Crawldb>>();
                    Crawldb base = new Crawldb();
                    base.setUrl(url);
                    base.setFromUrl(textCrawldbHtmlPageParamsTuple4._2().getFromUrl());
                    base.setRootUrl(textCrawldbHtmlPageParamsTuple4._2().getRootUrl());
                    base.setDepthfromSeed(textCrawldbHtmlPageParamsTuple4._2().getDepthfromSeed());
                    base.setHtml(((HtmlPage) upperPage).asXml().replaceFirst("<\\?xml version=\"1.0\" encoding=\"(.+)\"\\?>", "<!DOCTYPE html>"));
                    Tuple2 tuple2 = new Tuple2<Text,Crawldb>(new Text(url), base);
                    tuple2List.add(tuple2);
                    return tuple2List;*/
                    List<Tuple2<Text, Crawldb>> tuple2List = new ArrayList<Tuple2<Text, Crawldb>>();
                    Tuple2 tuple2 = new Tuple2<Text,Crawldb>(textCrawldbHtmlPageParamsTuple4._1(), textCrawldbHtmlPageParamsTuple4._2());
                    tuple2List.add(tuple2);
                    return tuple2List;
                }
            }
        };
        return result;
    }
}
