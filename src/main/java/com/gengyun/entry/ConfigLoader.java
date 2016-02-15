package com.gengyun.entry;


import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import com.gengyun.metainfo.Crawldb;
import com.gengyun.urlfilter.UrlDepthFilter;
import com.gengyun.utils.LogManager;
import com.gengyun.utils.ReadFromTachyon;
import org.apache.hadoop.io.Text;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;

/**
 * Created by lhj on 2015/11/9.
 * <p>初始化爬虫</p>
 */
public class ConfigLoader {
    private LogManager logger = new LogManager(ConfigLoader.class);
    private JSONObject jsonObject;
    //private final static PropertyHelper helper = new PropertyHelper("db");
   // private final static String tachyonUrl = helper.getValue("tachyonUrl");
    //public WriteType writeType = WriteType.CACHE_THROUGH;


    private void loadSeedConfigFile(String inputFilePath, String tid, String starttime,String tachyonUrl) {
        // JSONParser parser = new JSONParser();
        try {
            TachyonFS fs = TachyonFS.get(new TachyonURI(tachyonUrl));
            jsonObject = new JSONObject(ReadFromTachyon.getfilecontent(fs, inputFilePath));
            // List<String> seedingUrls = (List<String>) jsonObject.get("seeding_url");
            TachyonURI domainsPath = new TachyonURI("/SparkCrawler/" + tid + starttime + "/domains");

            if (fs.exist(domainsPath)) {
                fs.delete(domainsPath, true);
            }

            List<String> domains = new ArrayList<>();
            JSONArray jsonArray = jsonObject.getJSONArray("seeding_url");
            for (int i = 0; i < jsonArray.length(); i++) {
                URL url = new URL(jsonArray.getString(i));
                String domain = url.getHost();
                if (domain.startsWith("www")) {
                    domain = domain.replaceFirst("www.", "");
                }
                domains.add(domain);
            }

            /*for (String seedingUrl : seedingUrls) {
                URL url = new URL(seedingUrl);
                String domain = url.getHost();
                if (domain.startsWith("www")) {
                    domain = domain.replaceFirst("www.", "");
                }
                domains.add(domain);
            }*/
            OnSparkInstanceFactory.getSparkContext().parallelize(domains)
                    .saveAsTextFile(tachyonUrl + "/SparkCrawler/" + tid + starttime + "/domains");

        } catch (IOException e) {
            logger.logError("error when parsing config file: " + inputFilePath + "; " + e.toString());
        } catch (JSONException e) {
            logger.logError("error when parsing config file: " + inputFilePath + "; " + e.toString());
        }
    }

    public List<Tuple2<Text, Crawldb>> load(int depth, String tid, String starttime, int pass, String seedPath, String type,String tachyonUrl) {
        //TODO to use relative path of config file
        //PropertyHelper helper = new PropertyHelper("seedPath");
        loadSeedConfigFile(seedPath, tid, starttime,tachyonUrl);

        List<Tuple2<Text, Crawldb>> nextUrls = new ArrayList<Tuple2<Text, Crawldb>>();
        // InMemroySeenUrlFilter seenFilter = new InMemroySeenUrlFilter();
        try {
            TachyonFS fs = TachyonFS.get(new TachyonURI(tachyonUrl));
            if (fs.exist(new TachyonURI("/seenUrls"))) {
                fs.delete(new TachyonURI("/seenUrls"), true);
            }

            if (fs.exist(new TachyonURI("/toCrawl"))) {
                fs.delete(new TachyonURI("/toCrawl"), true);
            }

            if (fs.exist(new TachyonURI("/Crawled"))) {
                fs.delete(new TachyonURI("/Crawled"), true);
            }


            if (fs.exist(new TachyonURI("/downloaded"))) {
                fs.delete(new TachyonURI("/downloaded"), true);
            }

            Tuple2<Text, Crawldb> tuple2 = null;
            Tuple2<Text, Crawldb> crawldbTuple2 = null;


            List<Tuple2<Text, Crawldb>> initUrls = new ArrayList<Tuple2<Text, Crawldb>>();


            List<String> seedingUrls = new ArrayList<>();
            JSONArray jsonarray = jsonObject.getJSONArray("seeding_url");
            for (int i = 0; i < jsonarray.length(); i++) {
                seedingUrls.add(jsonarray.getString(i));
            }

            ArrayList<String> proccesedSeed = new ArrayList<String>();


            for (String seed : seedingUrls) {
                if (seed.endsWith("/")) {
                    seed = seed.substring(0, seed.lastIndexOf("/"));
                }

                Crawldb crawlDatum = new Crawldb();
                crawlDatum.setTid(tid);
                crawlDatum.setStarttime(starttime);
                crawlDatum.setPassed(pass);
                crawlDatum.setUrl(seed);
                crawlDatum.setType(type);
                crawlDatum.setRootUrl(seed);
                crawlDatum.setFromUrl(seed);
                crawlDatum.setDepthfromSeed(0);
                //crawlDatum.setFetched(false);


                tuple2 = new Tuple2<Text, Crawldb>(new Text(seed), crawlDatum);
                nextUrls.add(tuple2);

                initUrls.add(tuple2);

                proccesedSeed.add(seed);
            }


            //JavaSparkContext jsc = OnSparkInstanceFactory.getSparkContext();

            /***放入待爬取队列***/
            /*  OnSparkInstanceFactory.getNextURLQueueInstance().putNextUrls(jsc.parallelizePairs(nextUrls));*/

            // OnSparkInstanceFactory.getRedisToCrawlQue().putNextUrls(jsc.parallelizePairs(nextUrls));

            /*****放入已爬取队列****/
            // OnSparkInstanceFactory.getRddCrawledQueue().putRDD(jsc.parallelizePairs(initUrls));


            logger.logInfo("starting from baseUrl: " + nextUrls.toString());
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }

        UrlDepthFilter preFilter = new UrlDepthFilter(depth);
        OnSparkInstanceFactory.getPreExpansionFilterEnforcer().addFilter(preFilter);


        return nextUrls;

    }
}
