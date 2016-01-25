package com.gengyun.lsc.analysis.analysis;

import com.gengyun.lsc.analysis.urljudge.HtmlSort;
import com.gengyun.metainfo.Crawldb;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 15-12-22.
 */
public class TextAnalysis implements Serializable {
    private List<BaseTemplate> baseTemplates;

    private AnalysisArticle analysisArticle;

    public TextAnalysis(List<BaseTemplate> baseTemplates) {
        this.baseTemplates = baseTemplates;
        this.analysisArticle = new AnalysisArticle();
    }

    public PairFlatMapFunction<Tuple2<Text, Crawldb>, Text, Crawldb> analysis() {
        PairFlatMapFunction<Tuple2<Text, Crawldb>, Text, Crawldb> result = new PairFlatMapFunction<Tuple2<Text, Crawldb>, Text, Crawldb>() {
            @Override
            public Iterable<Tuple2<Text, Crawldb>> call(Tuple2<Text, Crawldb> tuple2) throws Exception {
                List<Tuple2<Text, Crawldb>> ans = new ArrayList<Tuple2<Text, Crawldb>>();
                List<BaseAnalysisURL> baseAnalysisURLList = new ArrayList<>();

                //初始化
                AnalysisNavigation analysisNavigation = new AnalysisNavigation();

                //获取url
                String url = tuple2._1().toString();

                Crawldb oldCrawldb = tuple2._2();
                String tid = oldCrawldb.getTid();
                int passed = oldCrawldb.getPassed();
                String type = oldCrawldb.getType();
                String rootUrl = oldCrawldb.getRootUrl();
                String html = oldCrawldb.getHtml();
                String title = oldCrawldb.getTitle();
                long date = oldCrawldb.getPublishtime();
                long depth = oldCrawldb.getDepthfromSeed();
                boolean tag = oldCrawldb.isTag();
                boolean fetched = oldCrawldb.isFetched();

//                System.out.println("*********url******** :" + url);
                BaseAnalysisURL oldUrl = new BaseAnalysisURL(url, title, date, html);


                //网页分类
                int sort = HtmlSort.getHtmlSort(url, html);

                //导航解析
                if (sort == 1) {
                    try {
                        baseAnalysisURLList = analysisNavigation.getUrlList(url, html);
                        for (BaseAnalysisURL baseAnalysisURL : baseAnalysisURLList) {
                            Crawldb newCrawdb = new Crawldb();
                            Text newtext = new Text();
                            newtext.set(baseAnalysisURL.getUrl());
                            newCrawdb.setTid(tid);
                            newCrawdb.setPassed(passed);
                            newCrawdb.setType(type);
                            newCrawdb.setUrl(baseAnalysisURL.getUrl());
                            newCrawdb.setText(baseAnalysisURL.getText());
                            newCrawdb.setTitle(baseAnalysisURL.getTitle());
                            newCrawdb.setRootUrl(rootUrl);
                            newCrawdb.setDepthfromSeed(depth + 1);
                            newCrawdb.setFromUrl(url);
                            newCrawdb.setPublishtime(baseAnalysisURL.getDate());
                            newCrawdb.setFetched(false);

                            ans.add(new Tuple2<Text, Crawldb>(newtext, newCrawdb));
                        }
                        oldCrawldb.setFetched(true);
                        oldCrawldb.setTag(false);
                        ans.add(new Tuple2<Text, Crawldb>(new Text(oldCrawldb.getUrl()), oldCrawldb));

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    //文章解析
                    oldUrl = analysisArticle.analysisArticle(oldUrl, baseTemplates);
                    oldCrawldb.setTitle(oldUrl.getTitle());
                    oldCrawldb.setPublishtime(oldUrl.getDate());
                    oldCrawldb.setText(oldUrl.getText());
                    oldCrawldb.setTag(true);
                    oldCrawldb.setFetched(true);

                    ans.add(new Tuple2<Text, Crawldb>(new Text(url), oldCrawldb));

                }
                return ans;
            }
        };

        return result;
    }


}
