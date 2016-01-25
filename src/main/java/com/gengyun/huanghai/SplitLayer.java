package com.gengyun.huanghai;

import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.gengyun.huanghai.recursive_width.executeTag;

/**
 * Created by root on 16-1-4.
 */
public class SplitLayer {

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
}
