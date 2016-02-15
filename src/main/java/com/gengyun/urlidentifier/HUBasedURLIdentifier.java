package com.gengyun.urlidentifier;

import com.gengyun.metainfo.BaseURL;
import com.gengyun.metainfo.CrawlDatum;
import com.gengyun.metainfo.Crawldb;
import com.gengyun.utils.CommonUtils;
import com.gengyun.utils.LogManager;
import org.apache.hadoop.io.Text;
import org.apache.html.dom.HTMLDocumentImpl;
import org.cyberneko.html.parsers.DOMFragmentParser;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.w3c.dom.DocumentFragment;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * htmlunit抽取链接
 * Created by hadoop on 2015/11/17.
 */


public class HUBasedURLIdentifier implements URLIdentifier, Serializable {
    private LogManager logger = new LogManager(HUBasedURLIdentifier.class);


    private DOMContentUtils utils;
    private HashSet<String> postfix;


    public HUBasedURLIdentifier(DOMContentUtils utils, HashSet<String> postfix) {
        this.utils = utils;
        this.parserImpl = "neko";
        this.defaultCharEncoding = "windows-1252";
        this.postfix = postfix;
    }

    @Override
    public List<BaseURL> extractUrls(BaseURL url) {
        return grabHTMLLinks(url);
    }

    @Override
    public List<Tuple2<Text, Crawldb>> extractCrawlDatumUrls(Tuple2<Text, Crawldb> base) {
        return grabHtmlLinksJsoup(base); //grabHTMLLinks(base);
    }


    public List<Tuple2<Text, Crawldb>> extractCrawlDatumUrls(Iterator<Tuple2<Text, Crawldb>> bases) {
        List<Tuple2<Text, Crawldb>> result = new ArrayList<>();
        while (bases.hasNext()) {
            grabHtmlLinksJsoup(bases.next(), result);
        }

        return result;
    }

    private void grabHtmlLinksJsoup(Tuple2<Text, Crawldb> tuple2, List<Tuple2<Text, Crawldb>> list) {
        try {
            Document doc = Jsoup.parse(tuple2._2().getHtml());
            Elements elements = doc.getElementsByTag("a");
            for (Element element : elements) {
                String absUrl = element.attr("href");
                String title = "";
                if (element.hasAttr("title")) {
                    title = element.attr("title");
                } else if (element.hasText()) {
                    title = element.text();
                }

                if (!absUrl.startsWith("javascript")) {
                    if (!absUrl.startsWith("Javascript")) {
                        if (absUrl.startsWith("http")) {
                            if (absUrl.endsWith("/")) {
                                absUrl = absUrl.substring(0, absUrl.lastIndexOf("/"));
                            }
                            putUrlToCrawl(absUrl, title, tuple2._2(), list);

                        } else if (absUrl.startsWith(".") || absUrl.startsWith("/")) {


                            if (tuple2._2().getUrl().endsWith(".cn") || absUrl.startsWith(".")) {

                                if (!tuple2._2().getUrl().endsWith(".cn")) {

                                    if (CommonUtils.parseSuffix(tuple2._2().getUrl().toString()) != null) {
                                        absUrl = new URL(new URL(tuple2._2().getUrl()), absUrl).toString();
                                    } else {

                                        absUrl = new URL(new URL(tuple2._2().getUrl().toString() + "/"), absUrl).toString();
                                    }

                                } else
                                    absUrl = new URL(new URL(tuple2._2().getUrl()), absUrl.substring(absUrl.indexOf("/"))).toString();


                            } else {
                                absUrl = new URL(new URL(tuple2._2().getUrl()), absUrl).toString();
                            }

                            if (absUrl.endsWith("/")) {
                                absUrl = absUrl.substring(0, absUrl.lastIndexOf("/"));
                            }
                            putUrlToCrawl(absUrl, title, tuple2._2(), list);

                        }

                    }
                }

            }

        } catch (MalformedURLException e) {
            System.out.println(e.toString() + "\t" + tuple2._2().getUrl());
        }

    }

    private List<Tuple2<Text, Crawldb>> grabHtmlLinksJsoup(Tuple2<Text, Crawldb> tuple2) {
        try {


            List<Tuple2<Text, Crawldb>> urlsToCrawl = new ArrayList<>();
            /*if (!(tuple2._2().getWebPage() instanceof HtmlUnitDocWebPage)) {
                logger.logError("HUBasedURLIdentifier must be used together with HtmlUnitDocWebPage");
                return urlsToCrawl;
            }*/
            Document doc = Jsoup.parse(tuple2._2().getHtml());
            Elements elements = doc.getElementsByTag("a");
            for (Element element : elements) {
                String absUrl = element.attr("href");
                String title = "";
                if (element.hasAttr("title")) {
                    title = element.attr("title");
                } else if (element.hasText()) {
                    title = element.text();
                }

                if (!absUrl.startsWith("javascript")) {
                    if (!absUrl.startsWith("Javascript")) {
                        if (absUrl.startsWith("http")) {
                            if (absUrl.endsWith("/")) {
                                absUrl = absUrl.substring(0, absUrl.lastIndexOf("/"));
                            }
                            putUrlToCrawl(absUrl, title, tuple2._2(), urlsToCrawl);

                        } else if (absUrl.startsWith(".") || absUrl.startsWith("/")) {


                            if (tuple2._2().getUrl().endsWith(".cn") || absUrl.startsWith(".")) {

                                if (!tuple2._2().getUrl().endsWith(".cn")) {

                                    if (CommonUtils.parseSuffix(tuple2._2().getUrl().toString()) != null) {
                                        absUrl = new URL(new URL(tuple2._2().getUrl()), absUrl).toString();
                                    } else {

                                        absUrl = new URL(new URL(tuple2._2().getUrl().toString() + "/"), absUrl).toString();
                                    }

                                } else
                                    absUrl = new URL(new URL(tuple2._2().getUrl()), absUrl.substring(absUrl.indexOf("/"))).toString();


                            } else {
                                absUrl = new URL(new URL(tuple2._2().getUrl()), absUrl).toString();
                            }

                            if (absUrl.endsWith("/")) {
                                absUrl = absUrl.substring(0, absUrl.lastIndexOf("/"));
                            }
                            putUrlToCrawl(absUrl, title, tuple2._2(), urlsToCrawl);


                        }

                    }
                }

            }


            return urlsToCrawl;
        } catch (MalformedURLException e) {
            System.out.println(e.toString() + "\t" + tuple2._2().getUrl());
        }

        return null;
    }


    private void putUrlToCrawl(String absUrl, String absUrlTitle, Crawldb url, List<Tuple2<Text, Crawldb>> urlsToCrawl) {

        String urlStr = absUrl;

        if (urlStr.endsWith("#")) {
            urlStr = urlStr.replaceAll("#", "");
        }
        if (urlStr.endsWith(".")) {
            urlStr = urlStr.substring(0, urlStr.lastIndexOf("."));
        }


        if (urlStr.endsWith("./")) {
            urlStr = urlStr.replaceAll("\\./", "");
            if (urlStr.endsWith("/")) {
                urlStr = urlStr.substring(0, urlStr.lastIndexOf("/"));
            }

        } else if (urlStr.contains("./")) {
            urlStr = urlStr.replaceAll("\\./", "");
            if (urlStr.endsWith("/")) {
                urlStr = urlStr.substring(0, urlStr.lastIndexOf("/"));
            }
        } else if (urlStr.endsWith("/")) {
            urlStr = urlStr.substring(0, urlStr.lastIndexOf("/"));
        }


        if (!postfix.contains(urlStr.substring(urlStr.lastIndexOf(".") + 1))) {
            Crawldb obj = new Crawldb();
            obj.setTitle(absUrlTitle);
            obj.setDepthfromSeed(url.getDepthfromSeed() + 1);
            obj.setFromUrl(url.getUrl());
            obj.setRootUrl(url.getRootUrl());
            obj.setUrl(urlStr);
            obj.setFetched(false);
            obj.setCrawltime(System.currentTimeMillis());
            urlsToCrawl.add(new Tuple2<Text, Crawldb>(new Text(urlStr), obj));
            logger.logInfo("discover url on page: " + absUrl + "\t" + absUrlTitle + "\t" + obj.getDepthfromSeed() + "\t" + url.getUrl());

        }


    }

    private void putUrlToCrawl(String absUrl, String absUrlTitle, CrawlDatum url, List<Tuple2<Text, CrawlDatum>> urlsToCrawl) {

        String urlStr = absUrl;

        if (urlStr.endsWith("#")) {
            urlStr = urlStr.replaceAll("#", "");
        }
        if (urlStr.endsWith(".")) {
            urlStr = urlStr.substring(0, urlStr.lastIndexOf("."));
        }


        if (urlStr.endsWith("./")) {
            urlStr = urlStr.replaceAll("\\./", "");
            if (urlStr.endsWith("/")) {
                urlStr = urlStr.substring(0, urlStr.lastIndexOf("/"));
            }

        } else if (urlStr.contains("./")) {
            urlStr = urlStr.replaceAll("\\./", "");
            if (urlStr.endsWith("/")) {
                urlStr = urlStr.substring(0, urlStr.lastIndexOf("/"));
            }
        } else if (urlStr.endsWith("/")) {
            urlStr = urlStr.substring(0, urlStr.lastIndexOf("/"));
        }


        if (!postfix.contains(urlStr.substring(urlStr.lastIndexOf(".") + 1))) {
            CrawlDatum obj = new CrawlDatum();
            obj.setTitle(absUrlTitle);
            obj.setDepthfromSeed(url.getDepthfromSeed() + 1);
            obj.setFromUrl(url.getUrl());
            obj.setRootUrl(url.getRootUrl());
            obj.setUrl(urlStr);
            obj.setFetched(false);
            obj.setCrawltime(System.currentTimeMillis());
            urlsToCrawl.add(new Tuple2<Text, CrawlDatum>(new Text(urlStr), obj));
            logger.logInfo("discover url on page: " + absUrl + "\t" + absUrlTitle + "\t" + obj.getDepthfromSeed() + "\t" + url.getUrl());

        }


    }

   /* private List<BaseURL> grabHTMLLinks(BaseURL url) {
        try {


            List<BaseURL> urlsToCrawl = new ArrayList<>();
            if (!(url.getPageContent() instanceof HtmlUnitDocWebPage)) {
                logger.logError("HUBasedURLIdentifier must be used together with HtmlUnitDocWebPage");
                return urlsToCrawl;
            }
            HtmlPage page = ((HtmlUnitDocWebPage) url.getPageContent()).getDoc();
            Document doc = Jsoup.parse(page.asXml().replaceFirst("<\\?xml version=\"1.0\" encoding=\"(.+)\"\\?>", "<!DOCTYPE html>"));
            Elements elements = doc.getElementsByTag("a");
            for (Element element : elements) {
                String absUrl = element.attr("href");


                String title = "";
                if (element.hasAttr("title")) {
                    title = element.attr("title");
                } else if (element.hasText()) {
                    title = element.text();
                }

                if (!absUrl.startsWith("javascript")) {
                    if (!absUrl.startsWith("Javascript")) {
                        if (absUrl.startsWith("http")) {
                            if (absUrl.endsWith("/")) {
                                absUrl = absUrl.substring(0, absUrl.lastIndexOf("/"));
                            }
                            putUrlToCrawl(absUrl, title, url, urlsToCrawl);
                            logger.logInfo("discover url on page: " + absUrl + "\t" + title + "\t" + url.getDepthFromSeed());
                        } else if (absUrl.startsWith(".") || absUrl.startsWith("/")) {


                            if (url.getUrl().toString().endsWith(".cn") || absUrl.startsWith(".")) {

                                if (!url.getUrl().toString().endsWith(".cn")) {

                                    if (CommonUtils.parseSuffix(url.getUrl().toString()) != null) {
                                        absUrl = new URL(url.getUrl(), absUrl).toString();
                                    } else {

                                        absUrl = new URL(new URL(url.getUrl().toString() + "/"), absUrl).toString();
                                    }

                                } else
                                    absUrl = new URL(url.getUrl(), absUrl.substring(absUrl.indexOf("/"))).toString();


                            } else {
                                absUrl = new URL(url.getUrl(), absUrl).toString();
                            }

                            if (absUrl.endsWith("/")) {
                                absUrl = absUrl.substring(0, absUrl.lastIndexOf("/"));
                            }
                            putUrlToCrawl(absUrl, title, url, urlsToCrawl);
                            logger.logInfo("discover url on page: " + absUrl + "\t" + title + "\t" + url.getDepthFromSeed());
                        }

                    }
                }


            }


            return urlsToCrawl;
        } catch (MalformedURLException e) {
            System.out.println(e.toString() + "\t" + url.getUrl().toString());
        }

        return null;

    }


    private void putUrlToCrawl(String absUrl, String absUrlTitle, BaseURL url, List<BaseURL> urlsToCrawl) {
        BaseURL obj = null;
        try {
            obj = new BaseURL(new URL(absUrl));
            obj.setTitle(absUrlTitle);
            obj.setDepthFromSeed(url.getDepthFromSeed() + 1);
            obj.setParentUrl(url.getUrl());
            obj.setCount(1L);
            urlsToCrawl.add(obj);
        } catch (MalformedURLException e) {
            logger.logError("exception during creating url: " + e.toString());
        }
    }*/


    private List<BaseURL> grabHTMLLinks(BaseURL base) {
        DocumentFragment root = null;
        HTMLMetaTags metaTags = new HTMLMetaTags();
        String text = "";
        String title = "";

        try {

            byte[] contentInOctets = base.getPageContent().getPageHtml().getBytes();
            InputSource input = new InputSource(new ByteArrayInputStream(contentInOctets));
            input.setEncoding("utf-8");
            root = parse(input);
        } catch (Exception e) {
            e.printStackTrace();
        }

        HTMLMetaProcessor.getMetaTags(metaTags, root, base.getUrl());

        if (!metaTags.getNoIndex()) {               // okay to index
            StringBuffer sb = new StringBuffer();

            utils.getText(sb, root);          // extract text
            text = sb.toString();
            sb.setLength(0);

            utils.getTitle(sb, root);         // extract title
            title = sb.toString().trim();
        }

        ArrayList<BaseURL> l = new ArrayList<BaseURL>();   // extract outlinks
        if (!metaTags.getNoFollow()) {              // okay to follow links

            URL baseTag = utils.getBase(root);

            utils.getOutlinks(base, l, root);

        }


        return l;

    }


    private List<Tuple2<Text, CrawlDatum>> grabHTMLLinks(Tuple2<Text, CrawlDatum> base) {
        DocumentFragment root = null;
        HTMLMetaTags metaTags = new HTMLMetaTags();
        String text = "";
        String title = "";
        URL url = null;

        try {
            url = new URL(base._1().toString());
            byte[] contentInOctets = base._2().getHtml().getBytes();
            InputSource input = new InputSource(new ByteArrayInputStream(contentInOctets));
            input.setEncoding("utf-8");
            root = parse(input);
        } catch (Exception e) {
            e.printStackTrace();
        }


        HTMLMetaProcessor.getMetaTags(metaTags, root, url);

        if (!metaTags.getNoIndex()) {               // okay to index
            StringBuffer sb = new StringBuffer();

            utils.getText(sb, root);          // extract text
            text = sb.toString();
            sb.setLength(0);

            utils.getTitle(sb, root);         // extract title
            title = sb.toString().trim();
        }

        ArrayList<Tuple2<Text, CrawlDatum>> l = new ArrayList<Tuple2<Text, CrawlDatum>>();   // extract outlinks
        if (!metaTags.getNoFollow()) {              // okay to follow links

            URL baseTag = utils.getBase(root);

            utils.getOutlinks(base, l, root, postfix);
        }


        return l;

    }

    private String parserImpl;
    private String defaultCharEncoding;

    private DocumentFragment parse(InputSource input) throws Exception {
        if (parserImpl.equalsIgnoreCase("tagsoup"))
            return parseTagSoup(input);
        else return parseNeko(input);
    }

    private DocumentFragment parseTagSoup(InputSource input) throws Exception {
        HTMLDocumentImpl doc = new HTMLDocumentImpl();
        DocumentFragment frag = doc.createDocumentFragment();
        DOMBuilder builder = new DOMBuilder(doc, frag);
        org.ccil.cowan.tagsoup.Parser reader = new org.ccil.cowan.tagsoup.Parser();
        reader.setContentHandler(builder);
        reader.setFeature(org.ccil.cowan.tagsoup.Parser.ignoreBogonsFeature, true);
        reader.setFeature(org.ccil.cowan.tagsoup.Parser.bogonsEmptyFeature, false);
        reader.setProperty("http://xml.org/sax/properties/lexical-handler", builder);
        reader.parse(input);
        return frag;
    }

    private DocumentFragment parseNeko(InputSource input) throws Exception {
        DOMFragmentParser parser = new DOMFragmentParser();
        try {
            parser.setFeature("http://cyberneko.org/html/features/scanner/allow-selfclosing-iframe",
                    true);
            parser.setFeature("http://cyberneko.org/html/features/augmentations",
                    true);
            parser.setProperty("http://cyberneko.org/html/properties/default-encoding",
                    defaultCharEncoding);
            parser.setFeature("http://cyberneko.org/html/features/scanner/ignore-specified-charset",
                    true);
            parser.setFeature("http://cyberneko.org/html/features/balance-tags/ignore-outside-content",
                    false);
            parser.setFeature("http://cyberneko.org/html/features/balance-tags/document-fragment",
                    true);

        } catch (SAXException e) {
        }
        // convert Document to DocumentFragment
        HTMLDocumentImpl doc = new HTMLDocumentImpl();
        doc.setErrorChecking(false);
        DocumentFragment res = doc.createDocumentFragment();
        DocumentFragment frag = doc.createDocumentFragment();
        parser.parse(input, frag);
        res.appendChild(frag);

        try {
            while (true) {
                frag = doc.createDocumentFragment();
                parser.parse(input, frag);
                if (!frag.hasChildNodes()) break;

                res.appendChild(frag);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

}
