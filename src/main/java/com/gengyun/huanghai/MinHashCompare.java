//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.gengyun.huanghai;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.jsoup.nodes.Document;

import java.io.IOException;

public class MinHashCompare {
    private static Tokenizer tokenizer = null;
    private static int hashBit = 1;
    private static int seed = 0;
    private static int num = 128;
    private static Analyzer analyzer;

    static {
        analyzer = MinHash.createAnalyzer(tokenizer, hashBit, seed, num);
    }

    public MinHashCompare() {
    }

    public static byte[] getMinHashValue(Document document) throws IOException {
        String text = document != null?document.toString():null;
        return MinHash.calculate(analyzer, text);
    }

    public static float getHtmlSimilarPercentage(Document document1, Document document2) throws IOException {
        String text1 = document1 != null?document1.toString():null;
        String text2 = document2 != null?document2.toString():null;
        byte[] minhash1 = MinHash.calculate(analyzer, text1);
        byte[] minhash2 = MinHash.calculate(analyzer, text2);
        return MinHash.compare(minhash1, minhash2);
    }

    public static float getBodySimilarPercentage(Document document1, Document document2) throws IOException {
        String text1 = document1 != null?document1.body().toString():null;
        String text2 = document2 != null?document2.body().toString():null;
        byte[] minhash1 = MinHash.calculate(analyzer, text1);
        byte[] minhash2 = MinHash.calculate(analyzer, text2);
        return MinHash.compare(minhash1, minhash2);
    }

    public static Boolean isSimilar(byte[] minhash1, byte[] minhash2) {
        return Boolean.valueOf(MinHash.compare(minhash1, minhash2) > 0.953125F);
    }

    public static Boolean isSimilar(byte[] minhash1, byte[] minhash2, float compareNum) {
        return Boolean.valueOf(MinHash.compare(minhash1, minhash2) > compareNum);
    }

    public static Boolean isHtmlSimilar(Document document1, Document document2) throws IOException {
        String text1 = document1 != null?document1.toString():null;
        String text2 = document2 != null?document2.toString():null;
        byte[] minhash1 = MinHash.calculate(analyzer, text1);
        byte[] minhash2 = MinHash.calculate(analyzer, text2);
        return Boolean.valueOf(MinHash.compare(minhash1, minhash2) > 0.953125F);
    }

    public static Boolean isHtmlSimilar(Document document1, Document document2, float compareNum) throws IOException {
        String text1 = document1 != null?document1.toString():null;
        String text2 = document2 != null?document2.toString():null;
        byte[] minhash1 = MinHash.calculate(analyzer, text1);
        byte[] minhash2 = MinHash.calculate(analyzer, text2);
        return Boolean.valueOf(MinHash.compare(minhash1, minhash2) > compareNum);
    }

    public static Boolean isBodySimilar(Document document1, Document document2) throws IOException {
        String text1 = document1 != null?document1.body().toString():null;
        String text2 = document2 != null?document2.body().toString():null;
        byte[] minhash1 = MinHash.calculate(analyzer, text1);
        byte[] minhash2 = MinHash.calculate(analyzer, text2);
        return Boolean.valueOf(MinHash.compare(minhash1, minhash2) > 0.953125F);
    }

    public static Boolean isBodySimilar(Document document1, Document document2, float compareNum) throws IOException {
        String text1 = document1 != null?document1.body().toString():null;
        String text2 = document2 != null?document2.body().toString():null;
        byte[] minhash1 = MinHash.calculate(analyzer, text1);
        byte[] minhash2 = MinHash.calculate(analyzer, text2);
        return Boolean.valueOf(MinHash.compare(minhash1, minhash2) > compareNum);
    }
}
