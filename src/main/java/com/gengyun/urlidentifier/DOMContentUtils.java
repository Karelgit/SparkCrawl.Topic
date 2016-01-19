/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gengyun.urlidentifier;

import com.gengyun.metainfo.BaseURL;
import com.gengyun.metainfo.CrawlDatum;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import scala.Tuple2;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * A collection of methods for extracting content from DOM trees.
 * <p/>
 * This class holds a few utility methods for pulling content out of
 * DOM nodes, such as getOutlinks, getText, etc.
 */
public class DOMContentUtils implements Serializable {

    public static class LinkParams implements Serializable {
        public String elName;
        public String attrName;
        public int childLen;

        public LinkParams(String elName, String attrName, int childLen) {
            this.elName = elName;
            this.attrName = attrName;
            this.childLen = childLen;
        }

        public String toString() {
            return "LP[el=" + elName + ",attr=" + attrName + ",len=" + childLen + "]";
        }
    }

    private HashMap<String, LinkParams> linkParams = new HashMap<String, LinkParams>();
    private SparkConf conf;

    public DOMContentUtils(SparkConf conf) {
        setConf(conf);
    }

    public void setConf(SparkConf conf) {
        // forceTags is used to override configurable tag ignoring, later on
        Collection<String> forceTags = new ArrayList<String>(1);

        //  this.conf = conf;
        linkParams.clear();
        linkParams.put("a", new LinkParams("a", "href", 1));
        linkParams.put("area", new LinkParams("area", "href", 0));
       /* if (conf.getBoolean("parser.html.form.use_action", true)) {
            linkParams.put("form", new LinkParams("form", "action", 1));
            if (conf.get("parser.html.form.use_action") != null)
                forceTags.add("form");
        }*/
        linkParams.put("frame", new LinkParams("frame", "src", 0));
        linkParams.put("iframe", new LinkParams("iframe", "src", 0));
        linkParams.put("script", new LinkParams("script", "src", 0));
        linkParams.put("link", new LinkParams("link", "href", 0));
        linkParams.put("img", new LinkParams("img", "src", 0));

        // remove unwanted link tags from the linkParams map
        /*String[] ignoreTags = conf.get("parser.html.outlinks.ignore_tags").split(",");
        for (int i = 0; ignoreTags != null && i < ignoreTags.length; i++) {
            if (!forceTags.contains(ignoreTags[i]))
                linkParams.remove(ignoreTags[i]);
        }*/
    }

    /**
     * This method takes a {@link StringBuffer} and a DOM {@link Node},
     * and will append all the content text found beneath the DOM node to
     * the <code>StringBuffer</code>.
     * <p/>
     * <p/>
     * <p/>
     * If <code>abortOnNestedAnchors</code> is true, DOM traversal will
     * be aborted and the <code>StringBuffer</code> will not contain
     * any text encountered after a nested anchor is found.
     * <p/>
     * <p/>
     *
     * @return true if nested anchors were found
     */
    public boolean getText(StringBuffer sb, Node node,
                           boolean abortOnNestedAnchors) {
        if (getTextHelper(sb, node, abortOnNestedAnchors, 0)) {
            return true;
        }
        return false;
    }


    /**
     * This is a convinience method, equivalent to {@link
     * #getText(StringBuffer, Node, boolean) getText(sb, node, false)}.
     */
    public void getText(StringBuffer sb, Node node) {
        getText(sb, node, false);
    }

    // returns true if abortOnNestedAnchors is true and we find nested
    // anchors
    private boolean getTextHelper(StringBuffer sb, Node node,
                                  boolean abortOnNestedAnchors,
                                  int anchorDepth) {
        boolean abort = false;
        NodeWalker walker = new NodeWalker(node);

        while (walker.hasNext()) {

            Node currentNode = walker.nextNode();
            String nodeName = currentNode.getNodeName();
            short nodeType = currentNode.getNodeType();

            if ("script".equalsIgnoreCase(nodeName)) {
                walker.skipChildren();
            }
            if ("style".equalsIgnoreCase(nodeName)) {
                walker.skipChildren();
            }
            if (abortOnNestedAnchors && "a".equalsIgnoreCase(nodeName)) {
                anchorDepth++;
                if (anchorDepth > 1) {
                    abort = true;
                    break;
                }
            }
            if (nodeType == Node.COMMENT_NODE) {
                walker.skipChildren();
            }
            if (nodeType == Node.TEXT_NODE) {
                // cleanup and trim the value
                String text = currentNode.getNodeValue();
                text = text.replaceAll("\\s+", " ");
                text = text.trim();
                if (text.length() > 0) {
                    if (sb.length() > 0) sb.append(' ');
                    sb.append(text);
                }
            }
        }

        return abort;
    }

    /**
     * This method takes a {@link StringBuffer} and a DOM {@link Node},
     * and will append the content text found beneath the first
     * <code>title</code> node to the <code>StringBuffer</code>.
     *
     * @return true if a title node was found, false otherwise
     */
    public boolean getTitle(StringBuffer sb, Node node) {

        NodeWalker walker = new NodeWalker(node);

        while (walker.hasNext()) {

            Node currentNode = walker.nextNode();
            String nodeName = currentNode.getNodeName();
            short nodeType = currentNode.getNodeType();

            if ("body".equalsIgnoreCase(nodeName)) { // stop after HEAD
                return false;
            }

            if (nodeType == Node.ELEMENT_NODE) {
                if ("title".equalsIgnoreCase(nodeName)) {
                    getText(sb, currentNode);
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * If Node contains a BASE tag then it's HREF is returned.
     */
    public URL getBase(Node node) {

        NodeWalker walker = new NodeWalker(node);

        while (walker.hasNext()) {

            Node currentNode = walker.nextNode();
            String nodeName = currentNode.getNodeName();
            short nodeType = currentNode.getNodeType();

            // is this node a BASE tag?
            if (nodeType == Node.ELEMENT_NODE) {

                if ("body".equalsIgnoreCase(nodeName)) { // stop after HEAD
                    return null;
                }

                if ("base".equalsIgnoreCase(nodeName)) {
                    NamedNodeMap attrs = currentNode.getAttributes();
                    for (int i = 0; i < attrs.getLength(); i++) {
                        Node attr = attrs.item(i);
                        if ("href".equalsIgnoreCase(attr.getNodeName())) {
                            try {
                                return new URL(attr.getNodeValue());
                            } catch (MalformedURLException e) {
                            }
                        }
                    }
                }
            }
        }

        // no.
        return null;
    }


    private boolean hasOnlyWhiteSpace(Node node) {
        String val = node.getNodeValue();
        for (int i = 0; i < val.length(); i++) {
            if (!Character.isWhitespace(val.charAt(i)))
                return false;
        }
        return true;
    }

    // this only covers a few cases of empty links that are symptomatic
    // of nekohtml's DOM-fixup process...
    private boolean shouldThrowAwayLink(Node node, NodeList children,
                                        int childLen, LinkParams params) {
        if (childLen == 0) {
            // this has no inner structure
            if (params.childLen == 0) return false;
            else return true;
        } else if ((childLen == 1)
                && (children.item(0).getNodeType() == Node.ELEMENT_NODE)
                && (params.elName.equalsIgnoreCase(children.item(0).getNodeName()))) {
            // single nested link
            return true;

        } else if (childLen == 2) {

            Node c0 = children.item(0);
            Node c1 = children.item(1);

            if ((c0.getNodeType() == Node.ELEMENT_NODE)
                    && (params.elName.equalsIgnoreCase(c0.getNodeName()))
                    && (c1.getNodeType() == Node.TEXT_NODE)
                    && hasOnlyWhiteSpace(c1)) {
                // single link followed by whitespace node
                return true;
            }

            if ((c1.getNodeType() == Node.ELEMENT_NODE)
                    && (params.elName.equalsIgnoreCase(c1.getNodeName()))
                    && (c0.getNodeType() == Node.TEXT_NODE)
                    && hasOnlyWhiteSpace(c0)) {
                // whitespace node followed by single link
                return true;
            }

        } else if (childLen == 3) {
            Node c0 = children.item(0);
            Node c1 = children.item(1);
            Node c2 = children.item(2);

            if ((c1.getNodeType() == Node.ELEMENT_NODE)
                    && (params.elName.equalsIgnoreCase(c1.getNodeName()))
                    && (c0.getNodeType() == Node.TEXT_NODE)
                    && (c2.getNodeType() == Node.TEXT_NODE)
                    && hasOnlyWhiteSpace(c0)
                    && hasOnlyWhiteSpace(c2)) {
                // single link surrounded by whitespace nodes
                return true;
            }
        }

        return false;
    }


    public void getOutlinks(BaseURL base, ArrayList<BaseURL> outlinks,
                            Node node) {

        NodeWalker walker = new NodeWalker(node);
        while (walker.hasNext()) {

            Node currentNode = walker.nextNode();
            String nodeName = currentNode.getNodeName();
            short nodeType = currentNode.getNodeType();
            NodeList children = currentNode.getChildNodes();
            int childLen = (children != null) ? children.getLength() : 0;

            if (nodeType == Node.ELEMENT_NODE) {

                nodeName = nodeName.toLowerCase();
                LinkParams params = (LinkParams) linkParams.get(nodeName);
                if (params != null) {
                    if (!shouldThrowAwayLink(currentNode, children, childLen, params)) {

                        StringBuffer linkText = new StringBuffer();
                        getText(linkText, currentNode, true);
                        if (linkText.toString().trim().length() == 0) {
                            // try harder - use img alt if present
                            NodeWalker subWalker = new NodeWalker(currentNode);
                            while (subWalker.hasNext()) {
                                Node subNode = subWalker.nextNode();
                                if (subNode.getNodeType() == Node.ELEMENT_NODE) {
                                    if (subNode.getNodeName().toLowerCase().equals("img")) {
                                        NamedNodeMap subAttrs = subNode.getAttributes();
                                        Node alt = subAttrs.getNamedItem("alt");
                                        if (alt != null) {
                                            String altTxt = alt.getTextContent();
                                            if (altTxt != null && altTxt.trim().length() > 0) {
                                                if (linkText.length() > 0) linkText.append(' ');
                                                linkText.append(altTxt);
                                            }
                                        }
                                    } else {
                                        // ignore other types of elements

                                    }
                                } else if (subNode.getNodeType() == Node.TEXT_NODE) {
                                    String txt = subNode.getTextContent();
                                    if (txt != null && txt.length() > 0) {
                                        if (linkText.length() > 0) linkText.append(' ');
                                        linkText.append(txt);
                                    }
                                }
                            }
                        }

                        NamedNodeMap attrs = currentNode.getAttributes();
                        String target = null;
                        boolean noFollow = false;
                        boolean post = false;
                        for (int i = 0; i < attrs.getLength(); i++) {
                            Node attr = attrs.item(i);
                            String attrName = attr.getNodeName();
                            if (params.attrName.equalsIgnoreCase(attrName)) {
                                target = attr.getNodeValue();
                            } else if ("rel".equalsIgnoreCase(attrName) &&
                                    "nofollow".equalsIgnoreCase(attr.getNodeValue())) {
                                noFollow = true;
                            } else if ("method".equalsIgnoreCase(attrName) &&
                                    "post".equalsIgnoreCase(attr.getNodeValue())) {
                                post = true;
                            }
                        }
                        if (target != null && !noFollow && !post)
                            try {

                                URL url = URLUtil.resolveURL(base.getUrl(), target);
                                BaseURL obj = new BaseURL(new URL(url.toString()));
                                obj.setTitle(linkText.toString().trim());
                                obj.setDepthFromSeed(base.getDepthFromSeed() + 1);
                                obj.setParentUrl(base.getUrl());
                                obj.setCount(1L);
                                outlinks.add(obj);
                            } catch (MalformedURLException e) {
                                // don't care
                            }
                    }
                    // this should not have any children, skip them
                    if (params.childLen == 0) continue;
                }
            }
        }
    }


    public void getOutlinks(Tuple2<Text, CrawlDatum> base, ArrayList<Tuple2<Text, CrawlDatum>> outlinks,
                            Node node, HashSet<String> postfix) {

        NodeWalker walker = new NodeWalker(node);
        while (walker.hasNext()) {

            Node currentNode = walker.nextNode();
            String nodeName = currentNode.getNodeName();
            short nodeType = currentNode.getNodeType();
            NodeList children = currentNode.getChildNodes();
            int childLen = (children != null) ? children.getLength() : 0;

            if (nodeType == Node.ELEMENT_NODE) {

                nodeName = nodeName.toLowerCase();
                LinkParams params = (LinkParams) linkParams.get(nodeName);
                if (params != null) {
                    if (!shouldThrowAwayLink(currentNode, children, childLen, params)) {

                        StringBuffer linkText = new StringBuffer();
                        getText(linkText, currentNode, true);
                        if (linkText.toString().trim().length() == 0) {
                            // try harder - use img alt if present
                            NodeWalker subWalker = new NodeWalker(currentNode);
                            while (subWalker.hasNext()) {
                                Node subNode = subWalker.nextNode();
                                if (subNode.getNodeType() == Node.ELEMENT_NODE) {
                                    if (subNode.getNodeName().toLowerCase().equals("img")) {
                                        NamedNodeMap subAttrs = subNode.getAttributes();
                                        Node alt = subAttrs.getNamedItem("alt");
                                        if (alt != null) {
                                            String altTxt = alt.getTextContent();
                                            if (altTxt != null && altTxt.trim().length() > 0) {
                                                if (linkText.length() > 0) linkText.append(' ');
                                                linkText.append(altTxt);
                                            }
                                        }
                                    } else {
                                        // ignore other types of elements

                                    }
                                } else if (subNode.getNodeType() == Node.TEXT_NODE) {
                                    String txt = subNode.getTextContent();
                                    if (txt != null && txt.length() > 0) {
                                        if (linkText.length() > 0) linkText.append(' ');
                                        linkText.append(txt);
                                    }
                                }
                            }
                        }

                        NamedNodeMap attrs = currentNode.getAttributes();
                        String target = null;
                        boolean noFollow = false;
                        boolean post = false;
                        for (int i = 0; i < attrs.getLength(); i++) {
                            Node attr = attrs.item(i);
                            String attrName = attr.getNodeName();
                            if (params.attrName.equalsIgnoreCase(attrName)) {
                                target = attr.getNodeValue();
                            } else if ("rel".equalsIgnoreCase(attrName) &&
                                    "nofollow".equalsIgnoreCase(attr.getNodeValue())) {
                                noFollow = true;
                            } else if ("method".equalsIgnoreCase(attrName) &&
                                    "post".equalsIgnoreCase(attr.getNodeValue())) {
                                post = true;
                            }
                        }
                        if (target != null && !noFollow && !post)
                            try {

                                URL baseUrl = new URL(base._1().toString());

                                URL url = URLUtil.resolveURL(baseUrl, target);
                               /*
                                BaseURL obj = new BaseURL(new URL(url.toString()));
                                obj.setTitle(linkText.toString().trim());
                                obj.setDepthFromSeed(base._2().getDepthfromSeed() + 1);
                                obj.setParentUrl(base.getUrl());
                                obj.setCount(1L);*/
                                String urlStr = url.toString();

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
                                    CrawlDatum crawlDatum = new CrawlDatum();
                                    crawlDatum.setUrl(urlStr);
                                    crawlDatum.setRootUrl(base._2().getRootUrl());
                                    crawlDatum.setFromUrl(baseUrl.toString());
                                    crawlDatum.setDepthfromSeed(base._2().getDepthfromSeed() + 1);
                                    crawlDatum.setCrawltime(System.currentTimeMillis());
                                    crawlDatum.setFetched(false);
                                    outlinks.add(new Tuple2<Text, CrawlDatum>(new Text(urlStr), crawlDatum));
                                }
                            } catch (MalformedURLException e) {
                                //skip
                            }
                    }
                    // this should not have any children, skip them
                    if (params.childLen == 0) continue;
                }
            }
        }
    }

}

