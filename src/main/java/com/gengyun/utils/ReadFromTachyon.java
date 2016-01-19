package com.gengyun.utils;

import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;

import java.io.IOException;

/**
 * Created by root on 16-1-14.
 */
public class ReadFromTachyon {
    public static String getfilecontent(TachyonFS tfs, String filepath) throws IOException {
        TachyonFile file = tfs.getFile(new TachyonURI(filepath));


        InStream in = file.getInStream(ReadType.CACHE);
        byte[] tempbytes = new byte[1024];
        int byteread = 0;
        StringBuffer sb = new StringBuffer();

        while ((byteread = in.read(tempbytes)) != -1) {
            sb.append(new String(tempbytes));
        }

        return sb.toString();
    }
}
