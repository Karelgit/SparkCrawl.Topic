package com.gengyun.huanghai.test;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Created by root on 16-1-25.
 */
public class ClickTest {
    public static void main(String[] args) {
        MyThread myThread = new MyThread();
        new Thread(myThread).start();
    }
}


class MyThread implements Runnable   {


    @Override
    public void run() {
        LogFactory.getFactory().setAttribute("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
        String url = "http://222.85.149.5:8080/WebCommon/test.jsp";
        WebClient webClient = new WebClient(BrowserVersion.CHROME);
        webClient.getOptions().setThrowExceptionOnScriptError(false);
        webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
        webClient.getOptions().setJavaScriptEnabled(true);
        webClient.setJavaScriptTimeout(3600 * 1000);
        webClient.getOptions().setRedirectEnabled(true);
        webClient.getOptions().setTimeout(5000);
        webClient.waitForBackgroundJavaScript(600 * 1000);
        webClient.setAjaxController(new NicelyResynchronizingAjaxController());
        try {
            for(int i=0; i<10000; i++)  {
                HtmlPage indexPage = webClient.getPage(url);
                System.out.println("i: " + i);
            }
            Thread.sleep(250);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}