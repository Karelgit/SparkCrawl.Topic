package com.gengyun.utils;

import tachyon.TachyonURI;
import tachyon.client.TachyonFS;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hadoop on 2015/11/25.
 */
public class CommonUtils {
    private final static PropertyHelper helper = new PropertyHelper("db");
    private final static String tachyonUrl = helper.getValue("tachyonUrl");
    final static Pattern pattern = Pattern.compile("\\S*[?]\\S*");

    /**
     * 获取链接的后缀名
     *
     * @return
     */
    public static String parseSuffix(String url) {

        try {
            Matcher matcher = pattern.matcher(url);

            String[] spUrl = url.toString().split("/");
            int len = spUrl.length;
            String endUrl = spUrl[len - 1];

            if (matcher.find()) {
                String[] spEndUrl = endUrl.split("\\?");
                return spEndUrl[0].split("\\.")[1];
            }
            return endUrl.split("\\.")[1];
        } catch (Exception e) {
            return null;
        }
    }


    public static void remove(String path) throws IOException {
        TachyonFS tfs = TachyonFS.get(new TachyonURI(tachyonUrl));
        if (tfs.exist(new TachyonURI(path))) {
            tfs.delete(new TachyonURI(path), true);
        }

    }

    public static void rename(String oldpath, String targetpath) throws IOException {
        TachyonFS tfs = TachyonFS.get(new TachyonURI(tachyonUrl));
        tfs.rename(new TachyonURI(oldpath), new TachyonURI(targetpath));

    }

    public static boolean exit(String path) throws IOException {
        TachyonFS tfs = TachyonFS.get(new TachyonURI(tachyonUrl));

        return tfs.exist(new TachyonURI(path));

    }

    public static void lockToCrawl() throws IOException {
        TachyonFS tfs = TachyonFS.get(new TachyonURI(tachyonUrl));
        tfs.createFile(new TachyonURI("/toCrawl/.lock"));
    }

    public static void unlockToCrawl() throws IOException {
        TachyonFS tfs = TachyonFS.get(new TachyonURI(tachyonUrl));
        if (tfs.exist(new TachyonURI("/toCrawl/.lock"))) {
            tfs.delete(new TachyonURI("/toCrawl/.lock"), true);
        }
    }

}
