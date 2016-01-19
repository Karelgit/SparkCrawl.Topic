//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.gengyun.huanghai;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

public class MinHash {
    private MinHash() {
    }

    public static float compare(int numOfBits, String str1, String str2) {
        return compare(numOfBits, BaseEncoding.base64().decode(str1), BaseEncoding.base64().decode(str2));
    }

    public static float compare(String str1, String str2) {
        return compare(BaseEncoding.base64().decode(str1), BaseEncoding.base64().decode(str2));
    }

    public static float compare(byte[] data1, byte[] data2) {
        return compare(data1.length * 8, data1, data2);
    }

    public static float compare(int numOfBits, byte[] data1, byte[] data2) {
        if (data1.length != data2.length) {
            return 0.0F;
        } else {
            int count = countSameBits(data1, data2);
            return (float) count / (float) numOfBits;
        }
    }

    protected static int countSameBits(byte[] data1, byte[] data2) {
        int count = 0;

        for (int i = 0; i < data1.length; ++i) {
            byte b1 = data1[i];
            byte b2 = data2[i];

            for (int j = 0; j < 8; ++j) {
                if ((b1 & 1) == (b2 & 1)) {
                    ++count;
                }

                b1 = (byte) (b1 >> 1);
                b2 = (byte) (b2 >> 1);
            }
        }

        return count;
    }

    public static HashFunction[] createHashFunctions(int seed, int num) {
        HashFunction[] hashFunctions = new HashFunction[num];

        for (int i = 0; i < num; ++i) {
            hashFunctions[i] = Hashing.murmur3_128(seed + i);
        }

        return hashFunctions;
    }

    public static byte[] calculate(Analyzer analyzer, String text) throws IOException {
        byte[] value = null;
        TokenStream stream = analyzer.tokenStream("minhash", new StringReader(text));
        Throwable var4 = null;

        try {
            CharTermAttribute var14 = (CharTermAttribute) stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            if (stream.incrementToken()) {
                String minhashValue = var14.toString();
                value = BaseEncoding.base64().decode(minhashValue);
            }

            stream.end();
        } catch (Throwable var141) {
            var4 = var141;
        } finally {
            if (stream != null) {
                if (var4 != null) {
                    try {
                        stream.close();
                    } catch (Throwable var13) {
                        var4.addSuppressed(var13);
                    }
                } else {
                    stream.close();
                }
            }

        }

        return value;
    }

    public static byte[] calculate(Data data) throws IOException {
        return calculate(data.analyzer, data.text);
    }

    public static byte[] calculate(Data[] data) throws IOException {
        int bitSize = 0;
        Data[] pos = data;
        int bitSet = data.length;

        int var16;
        for (var16 = 0; var16 < bitSet; ++var16) {
            Data var17 = pos[var16];
            bitSize += var17.numOfBits;
        }

        var16 = 0;
        FastBitSet var181 = new FastBitSet(bitSize);
        Data[] var18 = data;
        int var19 = data.length;

        for (int var6 = 0; var6 < var19; ++var6) {
            Data target1 = var18[var6];
            int count = 0;
            byte[] bytes = calculate(target1);
            byte[] var10 = bytes;
            int var11 = bytes.length;

            for (int var12 = 0; var12 < var11; ++var12) {
                byte b = var10[var12];
                byte bits = b;

                for (int j = 0; j < 8; ++j) {
                    var181.set(var16, (bits & 1) == 1);
                    ++var16;
                    ++count;
                    if (count >= target1.numOfBits) {
                        break;
                    }

                    bits = (byte) (bits >> 1);
                }
            }
        }

        return var181.toByteArray();
    }

    public static String toBinaryString(byte[] data) {
        if (data == null) {
            return null;
        } else {
            StringBuilder buf = new StringBuilder(data.length * 8);
            byte[] var2 = data;
            int var3 = data.length;

            for (int var4 = 0; var4 < var3; ++var4) {
                byte element = var2[var4];
                byte bits = element;

                for (int j = 0; j < 8; ++j) {
                    if ((bits & 128) == 128) {
                        buf.append('1');
                    } else {
                        buf.append('0');
                    }

                    bits = (byte) (bits << 1);
                }
            }

            return buf.toString();
        }
    }

    public static int bitCount(byte[] data) {
        int count = 0;
        byte[] var2 = data;
        int var3 = data.length;

        for (int var4 = 0; var4 < var3; ++var4) {
            byte element = var2[var4];
            byte bits = element;

            for (int j = 0; j < 8; ++j) {
                if ((bits & 1) == 1) {
                    ++count;
                }

                bits = (byte) (bits >> 1);
            }
        }

        return count;
    }

    public static Analyzer createAnalyzer(final Tokenizer tokenizer, final int hashBit, int seed, int num) {
        final HashFunction[] hashFunctions = createHashFunctions(seed, num);
        Analyzer minhashAnalyzer = new Analyzer() {
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Object baseTokenizer = tokenizer == null ? new WhitespaceTokenizer(Version.LUCENE_CURRENT, reader) : tokenizer;
                MinHashTokenFilter stream = new MinHashTokenFilter((TokenStream) baseTokenizer, hashFunctions, hashBit);
                return new TokenStreamComponents((Tokenizer) baseTokenizer, stream);
            }
        };
        return minhashAnalyzer;
    }

    public static Data newData(Analyzer analyzer, String text, int numOfBits) {
        return new Data(analyzer, text, numOfBits);
    }

    public static class Data {
        final int numOfBits;
        final String text;
        final Analyzer analyzer;

        Data(Analyzer analyzer, String text, int numOfBits) {
            this.numOfBits = numOfBits;
            this.text = text;
            this.analyzer = analyzer;
        }
    }
}
