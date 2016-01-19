package com.gengyun.urlidentifier;


import java.net.MalformedURLException;
import java.net.URL;

/**
 * Utility class for URL analysis
 */
public class URLUtil {


    public static URL resolveURL(URL base, String target)
            throws MalformedURLException {
        target = target.trim();


        if (target.startsWith("?")) {
            return fixPureQueryTargets(base, target);
        }

        return new URL(base, target);
    }

    static URL fixPureQueryTargets(URL base, String target)
            throws MalformedURLException {
        if (!target.startsWith("?")) return new URL(base, target);

        String basePath = base.getPath();
        String baseRightMost = "";
        int baseRightMostIdx = basePath.lastIndexOf("/");
        if (baseRightMostIdx != -1) {
            baseRightMost = basePath.substring(baseRightMostIdx + 1);
        }

        if (target.startsWith("?")) target = baseRightMost + target;

        return new URL(base, target);
    }

}
