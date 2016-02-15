package com.gengyun.utils;

/**
 * Created by Karel on 2015/12/16.
 */

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class FileReader {
    public static final List<String> readFile(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        List regexList = new ArrayList<String>();
        for (String line = br.readLine(); line != null; line = br.readLine()) {
            if(!line.startsWith("#"))   {
                regexList.add(line);
            }
        }
        br.close();
        return regexList;
    }
}