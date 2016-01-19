package com.gengyun.utils;

/**
 * Created by hadoop on 2015/11/9.
 */
public class PaceKeeper {

    public static void pause(){
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
