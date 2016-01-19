package com.gengyun.lsc.analysis.urljudge;

/**
 * Created by Administrator on 2015/9/17.
 */
public class HtmlSort {

    /**
     * 返回网页类型
     * @param url
     * @param html
     * @return
     */
   public static int getHtmlSort(String url ,String html) {

        int sort;

        sort = JudgeURL.getSortByURL(url);
        if (sort==0)
        {
            try {
                sort = JudgeDensity.getSortByDensity(html);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        return (sort);
    }





}
