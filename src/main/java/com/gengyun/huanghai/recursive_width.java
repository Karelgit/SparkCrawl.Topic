package com.gengyun.huanghai;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gengyun.utils.FileReader;
import org.apache.commons.logging.LogFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class recursive_width {

    //判断一个页面中子标签是否全部已经执行
    public static boolean allTagsExecuted(List<Tag> tagList) {
        Boolean flag = true;
        for (int i = 0; i < tagList.size(); i++) {
            flag = flag && (tagList.get(i).getStatus() == 2);
        }
        return flag;
    }

    //刚触发的页面，便签队列均未执行
    public static boolean noTagsExecuted(List<Tag> childTagList) {
        Boolean flag = true;
        for (int i = 0; i < childTagList.size(); i++) {
            flag = flag && (childTagList.get(i).getStatus() == 0);
        }
        return flag;
    }

    //点击标签，t为HtmlPage页面待选标签中即将执行的标签
    public static HtmlPage executeTag(HtmlPage hp, List<Tag> tagList, int t) {
        String[] xpathArray = tagList.get(t).getXpath().split("\\|");
        HtmlPage page = null;
        for (int i = 0; i < xpathArray.length; i++) {
            if (!xpathArray[i].isEmpty()) {
                HtmlElement element = (HtmlElement) hp.getByXPath(xpathArray[i]).get(0);
                try {
                    page = element.click();
                } catch (IOException e) {
                    System.out.println("获取页面超时！");
                    e.printStackTrace();
                }
            }
        }
        return page;
    }

    //针对Spark爬虫，栏目页判断，进点击程序
    public static boolean columnPageJudge(HtmlPage hp, List<String> regexList) {
        List<Tag> tagList = new ArrayList<Tag>();

        Document document = Jsoup.parse(hp.asXml());
        Document document1 = Jsoup.parse(hp.asXml());
        document.getElementsByTag("meta").remove();
        document.getElementsByTag("style").remove();
        document.getElementsByTag("script").remove();

        boolean columnFlag = false;
        for (int i = 0; i < regexList.size(); i++) {
            Pattern p = Pattern.compile("</?\\w+((\\s+\\w+(\\s*=\\s*(?:\".*?\"|'.*?'|[^'\">\\s]+))?)+\\s*|\\s*)/?>");
            String temp = document.toString().replaceAll("\\r|\\t|\\n|\\?", "");
            Matcher m = p.matcher(temp);
            while (m.find()) {
                if(m.group().matches(regexList.get(i))) {
                    String xpath = XpathParser.getXpath(document1, m.group()).get(0) + "|";
                    columnFlag = true;
                }
            }
        }
        return columnFlag;
    }

    public static List<Tag> initTagList(HtmlPage hp, List<String> regexList, WebClient webClient) {
        List<Tag> tagList = new ArrayList<Tag>();
        Document document = Jsoup.parse(hp.asXml());
        Document document1 = Jsoup.parse(hp.asXml());
        document.getElementsByTag("meta").remove();
        document.getElementsByTag("style").remove();
        document.getElementsByTag("script").remove();

        for (int i = 0; i < regexList.size(); i++) {
            Pattern p = Pattern.compile("</?\\w+((\\s+\\w+(\\s*=\\s*(?:\".*?\"|'.*?'|[^'\">\\s]+))?)+\\s*|\\s*)/?>");
            String temp = document.toString().replaceAll("\\r|\\t|\\n|\\?", "");
            Matcher m = p.matcher(temp);
            while (m.find()) {
                if(m.group().matches(regexList.get(i))) {
                    String xpath = XpathParser.getXpath(document1, m.group()).get(0) + "|";
//                    System.out.println("xpath: " + xpath);
                    Tag tag = new Tag();
                    tag.setStatus(0);
                    tag.setXpath(xpath);
                    tagList.add(tag);
//                    System.out.println("tag包括：" + m.group());
                }
            }
        }
        return tagList;
    }

    public static boolean newPageJudge(List<String> pageList,HtmlPage htmlPage)    {
        boolean uniqueFlag = true;
        Document doc = Jsoup.parse(htmlPage.asXml());
        if(pageList.size() == 0) {
            uniqueFlag = true;
        }else {
            for(int i=0; i<pageList.size(); i++)   {
                MinHashCompare minHashCompare = new MinHashCompare();
                Document inner_doc = Jsoup.parse(pageList.get(i));
                try {
                    uniqueFlag = uniqueFlag && !minHashCompare.isHtmlSimilar(inner_doc,doc,0.999f);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return uniqueFlag;
    }

    public static List<String> traverse_width(int k, int layers,String url, List<String> regexList, Map<Integer,List<Tag>> tagListBlock,WebClient webClient, HtmlPage upperPage, boolean firstEntry) throws  Exception{
        List<String> pageList = new ArrayList<String>();
        pageList.add(upperPage.asXml());
        int num = k;
        System.out.print("第" + (num-k+1) + "层累计页数：");
        System.out.println(pageList.size());
        while (k > 0)   {
            if(k+1 <= layers)  {
                for(int i=0; i < tagListBlock.get(k+1).size(); i++) {
                    HtmlPage indexPage = null;
                    if(firstEntry == true)  {
                        indexPage = upperPage;
                        firstEntry = false;
                    }else   {
                        indexPage = webClient.getPage(url);
                    }

                    HtmlPage exhp = executeTag(indexPage, tagListBlock.get(k + 1), i);
                    if(newPageJudge(pageList,exhp)) {
                        pageList.add(exhp.asXml());
                        System.out.print("第" + (num-k+1) + "层累计页数：");
                        System.out.println(pageList.size());
                        if(k>1) {
                            List<Tag> initTagList = initTagList(exhp, regexList, webClient);
                            for(int j=0; j < initTagList.size(); j++) {
                                Tag tag = new Tag();
                                tag.setXpath(tagListBlock.get(k + 1).get(i).getXpath() + initTagList.get(j).getXpath() + "|");
                                tag.setStatus(1);
                                tagListBlock.get(k).add(tag);
                            }
                        }
                    }
                }
            }else {
                tagListBlock.put(k,initTagList(upperPage,regexList,webClient));
            }

            k--;//layers param decrease
        }
        System.out.println("page size: " + pageList.size());
        /*for (HtmlPage htmlPage : pageList) {
            System.out.println(htmlPage.asText());
        }*/
        return pageList;
    }

    public static Params getDynamicPages(String Url, List<String> regexList, int recalldepth) throws Exception {
        LogFactory.getFactory().setAttribute("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
        String url = Url;
        WebClient webClient = new WebClient(BrowserVersion.CHROME);
        webClient.getOptions().setThrowExceptionOnScriptError(false);
        webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
        webClient.getOptions().setJavaScriptEnabled(true);
        webClient.setJavaScriptTimeout(3600 * 1000);
        webClient.getOptions().setRedirectEnabled(true);
        webClient.getOptions().setTimeout(3600 * 1000);
        webClient.waitForBackgroundJavaScript(600 * 1000);
        webClient.setAjaxController(new NicelyResynchronizingAjaxController());
        //k,控制访问层数;m,叶子节点TagList循环控制
        Params params = new Params();
        params.setK(recalldepth);
        params.setLayer(recalldepth);
        Map<Integer, List<Tag>> tagListBlock = new HashMap<Integer, List<Tag>>();
        //初始化标签列表
        for (int i = params.getK(); i > 0; i--) {
            tagListBlock.put(i, new ArrayList<Tag>());
        }
        params.setTagListBlock(tagListBlock);
        params.setWebClient(webClient);
        return params;
    }

    public static void main(String[] args) throws Exception {

        LogFactory.getFactory().setAttribute("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
//        贵阳
//          String url = "http://www.gygov.gov.cn/col/col10682/index.html";
//        String url = "http://www.gygov.gov.cn/col/col18321/index.html";
//        String url = "http://www.gygov.gov.cn/col/col18321/index.html";
//        String url = "http://www.gygov.gov.cn/col/col10683/index.html";
//        String url = "http://www.gygov.gov.cn/col/col10687/index.html";

//        String url = "http://www.nanming.gov.cn/nmxw/gzdt/zwxx/index.shtml";
//        String url = "http://gz.hrss.gov.cn/col/col43/index.html";
//        String url = "http://www.gzdpc.gov.cn/col/col396/index.html";
//        String url = "http://gzgy.lss.gov.cn/col/col78/index.html";
        //都匀
//        String url = "http://www.duyun.gov.cn/fzlm/hdmb/xxgkml/xxgklb/index.shtml?organId=2,082&id=1";
        //贵州发改委
//        String url = "http://www.gzdpc.gov.cn/col/col406/index.html";
        String url = "http://www.qxn.gov.cn/OrgArtList/QxnGov.XMJ/QxnGov.XMJ.File/1.html";
//        String url = "http://www.gzdpc.gov.cn/col/col406/index.html";
        WebClient webClient = new WebClient(BrowserVersion.CHROME);
        webClient.getOptions().setThrowExceptionOnScriptError(false);
        webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
        webClient.getOptions().setJavaScriptEnabled(true);
        webClient.setJavaScriptTimeout(3600 * 1000);
        webClient.getOptions().setRedirectEnabled(true);
        webClient.getOptions().setTimeout(3600 * 1000);
        webClient.waitForBackgroundJavaScript(600 * 1000);
        webClient.setAjaxController(new NicelyResynchronizingAjaxController());
        HtmlPage indexPage = webClient.getPage(url);

        Long t1 = System.currentTimeMillis();
        List<String> regexList = new ArrayList<>();
        try {
            regexList = FileReader.readFile("/opt/regex.config");
        } catch (Exception e) {
            e.printStackTrace();
        }
        boolean firstEntry = true;
        Params params = recursive_width.getDynamicPages(url, regexList,3);
        /*Map<Integer, List<Tag>> tagListBlock1 = getTagListBlock(params.getK(), params.getLayer(), url, regexList, params.getTagListBlock(), params.getWebClient());
        params.setTagListBlock(tagListBlock1);
        traverseClick(params.getM(), params.getK(), url, indexPage, params.getTagListBlock(), params.getWebClient(), firstEntry);*/
        traverse_width(params.getK(),params.getLayer(),url,regexList,params.getTagListBlock(),params.getWebClient(),indexPage,firstEntry);
        Long t2 = System.currentTimeMillis();
        System.out.println(t2 - t1);
    }
}


