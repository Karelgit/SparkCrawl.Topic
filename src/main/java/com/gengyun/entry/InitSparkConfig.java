package com.gengyun.entry;

import com.gengyun.lsc.analysis.analysis.BaseTemplate;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.ReadFromTachyon;
import org.apache.spark.SparkConf;
import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.thrift.ClientFileInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * 初始化Spark配置
 * Created by lhj on 15-12-11.
 */
public class InitSparkConfig {
    private SparkConf sparkConf;
    private HashSet<String> postfix;
    //private final static PropertyHelper helper = new PropertyHelper("db");
    //private final static String tachyonUrl = helper.getValue("tachyonUrl");

    private List<BaseTemplate> listTemplate;

    private List<String> regexList;

    private List<String> protocols;
    private int recalldepth;

    public SparkConf getSparkConf() {
        return sparkConf;
    }

    public void setSparkConf(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    public HashSet<String> getPostfix() {
        return postfix;
    }

    public void setPostfix(HashSet<String> postfix) {
        this.postfix = postfix;
    }

    public List<BaseTemplate> getListTemplate() {
        return listTemplate;
    }

    public void setListTemplate(List<BaseTemplate> listTemplate) {
        this.listTemplate = listTemplate;
    }

    public List<String> getRegexList() {
        return regexList;
    }

    public void setRegexList(List<String> regexList) {
        this.regexList = regexList;
    }

    public List<String> getProtocols() {
        return protocols;
    }

    public void setProtocols(List<String> protocols) {
        this.protocols = protocols;
    }

    public int getRecalldepth() {
        return recalldepth;
    }

    public void setRecalldepth(int recalldepth) {
        this.recalldepth = recalldepth;
    }

    public InitSparkConfig(String appname, String mode, int recalldepth, String templatesDir,String clickregexDir, String protocolDir, String postregexDir,String tachyonUrl) {
        try {
            TachyonFS tfs = TachyonFS.get(new TachyonURI(tachyonUrl));
            SparkConf sparkConf = new SparkConf().setAppName(appname).set("spark.externalBlockStore.url", tachyonUrl).set("fs.tachyon-ft.impl", "tachyon.hadoop.TFSFT")/*.set("spark.executor.memory", "6g").set("spark.driver.memory", "3g")*/.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(new Class<?>[]{
                    Class.forName("org.apache.hadoop.io.Text"),
                    Crawldb.class
            });

            if (mode.equals("local")) {
                sparkConf.setMaster("local[*]");
            }

            setSparkConf(sparkConf);
            HashSet<String> postFilter = new HashSet<String>(Arrays.asList(new String[]{"GIF", "gif", "jpg", "png", "css", "ico", "js", "doc", "ppt", "xls", "rar", "pdf"}));

            String str;

            listTemplate = new ArrayList<>();
            regexList = new ArrayList<>();
            protocols = new ArrayList<>();
            postfix = new HashSet<String>();
            TachyonFS fs = TachyonFS.get(new TachyonURI(tachyonUrl));

            //读取模版
            //File templatesParentFile = new File("/opt/SparkCrawler/template");
            List<ClientFileInfo> clientFileInfos = tfs.listStatus(new TachyonURI(templatesDir));

            if (clientFileInfos != null) {
                for (ClientFileInfo templateFile : clientFileInfos) {
                    List<String> tokens = new ArrayList<>();
                    String domain = new String();
                    TachyonFile file = tfs.getFile(new TachyonURI(templateFile.getPath()));
                    InStream in = file.getInStream(ReadType.CACHE);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    domain = reader.readLine();
                    while ((str = reader.readLine()) != null) tokens.add(str);
                    reader.close();
                    listTemplate.add(new BaseTemplate(domain, tokens));
                }
            }

            setListTemplate(listTemplate);

            /*List<ClientFileInfo> clickregexdir = tfs.listStatus(new TachyonURI(clickregexDir));
            if (clickregexdir != null) {
                for (ClientFileInfo clickregexFile : clickregexdir) {
                    TachyonFile file = tfs.getFile(new TachyonURI(clickregexFile.getPath()));
                    InStream in = file.getInStream(ReadType.CACHE);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    while ((str = reader.readLine()) != null) regexList.add(str);
                    reader.close();
                }
            }*/


            String clickRegexStr = ReadFromTachyon.getfilecontent(fs, clickregexDir);
            String regex[] = clickRegexStr.split("\n");
            for (String s1 : regex) {
                if(s1 != "")    {
                    regexList.add(s1.trim());
                }
            }

            String protocolStr = ReadFromTachyon.getfilecontent(fs, protocolDir);
            String protocol[] = protocolStr.split("\n");
            for (String s1 : protocol) {
                if(s1 != "")    {
                    protocols.add(s1.trim());
                }
            }

            String postFixStr = ReadFromTachyon.getfilecontent(fs, postregexDir);
            String postArray[] = postFixStr.split("\n");
            for (String s1 : postArray) {
                if(s1 != "")    {
                    postfix.add(s1.trim());
                }
            }

            setRegexList(regexList);
            setRecalldepth(recalldepth);
            setProtocols(protocols);
            setPostfix(postfix);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
