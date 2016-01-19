package com.gengyun.huanghai;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ting on 10/13/2015.
 */
public class HtmlPreprocess {

    private static final String regEx_script = "<script[^>]*?>[\\s\\S]*?<\\/script>";
    private static final String regEx_style = "<style[^>]*?>[\\s\\S]*?<\\/style>";
    private static final String regEx_html = "<[^>]+>";
    private static final String regEx_space = "\\s*|\t|\r|\n";
    private static final String regEx_entity = "&([^;]+;?)";


    public static String delHTMLTag(String htmlStr) {
        Pattern p_script = Pattern.compile(regEx_script, Pattern.CASE_INSENSITIVE);
        Matcher m_script = p_script.matcher(htmlStr);
        htmlStr = m_script.replaceAll("");

        Pattern p_style = Pattern.compile(regEx_style, Pattern.CASE_INSENSITIVE);
        Matcher m_style = p_style.matcher(htmlStr);
        htmlStr = m_style.replaceAll("");

        Pattern p_html = Pattern.compile(regEx_html, Pattern.CASE_INSENSITIVE);
        Matcher m_html = p_html.matcher(htmlStr);
        htmlStr = m_html.replaceAll("");

        Pattern p_space = Pattern.compile(regEx_space, Pattern.CASE_INSENSITIVE);
        Matcher m_space = p_space.matcher(htmlStr);
        htmlStr = m_space.replaceAll("");

        Pattern p_entity = Pattern.compile(regEx_entity, Pattern.CASE_INSENSITIVE);
        Matcher m_entity = p_entity.matcher(htmlStr);
        htmlStr = m_entity.replaceAll("");

        return htmlStr.trim();
    }
}
