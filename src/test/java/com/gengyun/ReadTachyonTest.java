package com.gengyun;

import org.codehaus.jettison.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;
import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.thrift.ClientFileInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by root on 16-1-14.
 */
public class ReadTachyonTest {

    @Test
    public void testReadFromTachyon() throws Exception {

        TachyonFS tachyonClient = TachyonFS.get(new TachyonURI("tachyon://localhost:19998"));
        TachyonFile file = tachyonClient.getFile("/SparkCrawler/SparkCrawler201601081341/seeds/seed.json");


        InStream in = file.getInStream(ReadType.CACHE);
        byte[] tempbytes = new byte[1024];
        int byteread = 0;
        StringBuffer sb = new StringBuffer();

        while ((byteread = in.read(tempbytes)) != -1) {
            sb.append(new String(tempbytes));
        }
        JSONObject jsonObject= new JSONObject(sb.toString());


        System.out.println(jsonObject.getJSONArray("seeding_url"));
        in.close();
        tachyonClient.close();
    }

    @Test
    public void ListTachyonDir() throws IOException {
        TachyonFS tfs = TachyonFS.get(new TachyonURI("tachyon://localhost:19998"));

        List<ClientFileInfo> clientFileInfos = tfs.listStatus(new TachyonURI("/SparkCrawler/clickregex/"));
        String str;
        List<String> tokens = new ArrayList<>();
        if (clientFileInfos != null) {
            for (ClientFileInfo templateFile : clientFileInfos) {
                TachyonFile file = tfs.getFile(new TachyonURI(templateFile.getPath()));
                InStream in = file.getInStream(ReadType.CACHE);
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                while ((str = reader.readLine()) != null) tokens.add(str);
                reader.close();
            }
        }

        for (String token : tokens) {
            System.out.print(token+"\t");
        }



    }
}