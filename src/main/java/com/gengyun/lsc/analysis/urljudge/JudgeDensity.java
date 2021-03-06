package com.gengyun.lsc.analysis.urljudge;


import com.gengyun.lsc.analysis.tool.AnalysisTool;
import com.gengyun.lsc.analysis.tool.JsoupHtml;

import java.io.IOException;
import java.util.List;

/**
 * Created by leishichi on 2015/9/15.
 */

public class JudgeDensity
{

    private final static double DENSITY_THRESHOLDS = 0.06;
    private static double sumKey = 0;
    private static int linkTextNumber;


    public static int getSortByDensity(String html) throws IOException
    {
        int linkNumber=0;
        int textNumber=0;
        double densityKey;

        //将网页转换为token
        List<String> tokens= JsoupHtml.getHtmlTokens(html);


        //������ҳ���ҳ�������������������
        String token,nextToken;
        int sort;
        for (int i = 0; i <tokens.size() ; i++)
        {
            token=tokens.get(i);
            sort= AnalysisTool.getMarketSort(token);
            switch (sort)
            {
                case 0:textNumber+=token.length();break;
                case 2:{
                    linkNumber++;
                    nextToken=tokens.get(i+1);
                    if (AnalysisTool.getMarketSort(nextToken)==0) i++;
                }
                break;
            }
        }

        densityKey=(linkNumber+0.0)/(textNumber+0.0);
        if (densityKey >= DENSITY_THRESHOLDS) return (1);
        else return (-1);
    }







}
