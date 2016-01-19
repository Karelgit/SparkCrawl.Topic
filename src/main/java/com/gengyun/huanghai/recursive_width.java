package com.gengyun.huanghai;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.DomElement;
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
            // System.out.println(xpathArray[i].toString());
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

    //打印HtmlPage的页码
    public static String printPageNo(HtmlPage hp) {
        List<String> pageMark = new ArrayList<>();
        pageMark.add("//*[@id=\"11627\"]/table/tbody/tr/td/table/tbody/tr/td[6]/input");
        pageMark.add("/html/body/div[2]/div[2]/div[2]/p/span[1]");
        DomElement de = null;
        for (String s : pageMark) {
            if(hp.getByXPath(s).size() > 0)    {
                de = (DomElement) hp.getByXPath(s).get(0);
                break;
            }
        }

       /* //贵阳
        String pNoXpath1 = "/*//*[@id=\"11627\"]/table/tbody/tr/td/table/tbody/tr/td[6]/input";
        //南明
//        String pNoXpath2 = "/html/body/div[2]/div[2]/div[2]/p/span[1]";

        DomElement pageNo1 = (DomElement) hp.getByXPath(pNoXpath1).get(0);*/
        return "第".concat(de.asText()).concat("页");
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

    public static List<Tag> initTagList(String url, List<String> regexList, WebClient webClient) {
        List<Tag> tagList = new ArrayList<Tag>();
        HtmlPage hp = null;
        try {
            for (String s : regexList) {
                System.out.println(s);
            }
            hp = webClient.getPage(url);
        } catch (Exception e) {
            e.printStackTrace();
        }

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
                    System.out.println("xpath: " + xpath);
                    Tag tag = new Tag();
                    tag.setStatus(0);
                    tag.setXpath(xpath);
                    tagList.add(tag);
                    System.out.println("tag包括：" + m.group());
                }
            }
        }
        System.out.println("找到的tag数：" + tagList.size());
        return tagList;
    }

    //方法一：获得tagListBlock
    public static Map<Integer, List<Tag>> getTagListBlock(int k, int layers, String url, List<String> regexList, Map<Integer, List<Tag>> tagListBlock, WebClient webClient) {
        List<Tag> initTagList = initTagList(url, regexList, webClient);
        if(initTagList.size() !=0)  {
            int kc = k;
            while (kc > 1) {
                if (kc + 1 <= layers) {
                    for (int i = 0; i < tagListBlock.get(kc + 1).size(); i++) {
                        for (int j = 0; j < initTagList.size(); j++) {
                            Tag tag = new Tag();
                            tag.setXpath(tagListBlock.get(kc + 1).get(i).getXpath() + initTagList.get(j).getXpath() + "|");
                            tag.setStatus(1);
                            tagListBlock.get(kc).add(tag);
                        }
                    }
                } else {
                    tagListBlock.put(k, initTagList);
                }
                kc--;
            }
            return tagListBlock;
        }else   {
            return tagListBlock;
        }

    }

    //方法二：点击标签过程
    public static List<String> traverseClick(int m, int k, String url, HtmlPage upperPage,
                                               Map<Integer, List<Tag>> tagListBlock, WebClient webClient, boolean firstEntry) throws Exception {
        List<String> pageList = new ArrayList<String>();

        if(tagListBlock.get(k).size() == 0)   {
//            System.out.println(printPageNo(upperPage)+"已加入pageList");
            pageList.add(upperPage.asXml());
        }else   {
//            System.out.println(printPageNo(upperPage)+"已加入pageList");
            pageList.add(upperPage.asXml());
            int kd = k;
            while(kd > 1) {
                for (; m < tagListBlock.get(kd).size(); m++) {
                    HtmlPage indexPage = null;
                    if (firstEntry) {
                        indexPage = upperPage;
                        firstEntry = false;
                    } else {
                        try {
                            indexPage = webClient.getPage(url);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    HtmlPage htmlPage = executeTag(indexPage, tagListBlock.get(kd), m);
//                    printPageNo(htmlPage);
                    Document doc = Jsoup.parse(htmlPage.asXml());
                    if (pageList.isEmpty()) {
                        pageList.add(htmlPage.asXml());
                    } else {
                        boolean uniqueFlag = true;
                        for (int i = 0; i < pageList.size(); i++) {
                            MinHashCompare minHashCompare = new MinHashCompare();
                            Document inner_doc = Jsoup.parse(pageList.get(i));
                            try {
                                uniqueFlag = uniqueFlag && !minHashCompare.isHtmlSimilar(inner_doc, doc, 0.999999999f);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        if (uniqueFlag == true) {
//                            System.out.println(printPageNo(htmlPage)+"已加入pageList");
                            pageList.add(htmlPage.asXml());
                        }
                    }
                }
                m = 0;
                kd--;
            }
        }
        System.out.println("pageList size:" + pageList.size());
        return pageList;
    }


    public static Params getDynamicPages(String Url, List<String> regexList,int recall_depth) throws Exception {
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
        params.setM(0);
        params.setK(recall_depth);
        params.setLayer(recall_depth);
        Map<Integer, List<Tag>> tagListBlock = new HashMap<Integer, List<Tag>>();
        //初始化标签列表
        for (int i = params.getK(); i > 0; i--) {
            tagListBlock.put(i, new ArrayList<Tag>());
        }
        params.setTagListBlock(tagListBlock);
        params.setWebClient(webClient);
        return params;
    }

}


