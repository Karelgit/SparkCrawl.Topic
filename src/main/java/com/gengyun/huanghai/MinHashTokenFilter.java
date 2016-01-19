//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.gengyun.huanghai;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.io.BaseEncoding;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.Arrays;

public class MinHashTokenFilter extends TokenFilter {
    private final CharTermAttribute termAttr = (CharTermAttribute) this.addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAttr = (PositionIncrementAttribute) this.addAttribute(PositionIncrementAttribute.class);
    private final OffsetAttribute offsetAttr = (OffsetAttribute) this.addAttribute(OffsetAttribute.class);
    private HashFunction[] hashFunctions;
    private int hashBit;
    private long[] minHashValues;
    private String minHash;

    public MinHashTokenFilter(TokenStream input, HashFunction[] hashFunctions, int hashBit) {
        super(input);
        this.hashFunctions = hashFunctions;
        this.hashBit = hashBit;
        this.minHashValues = new long[hashFunctions.length];
    }

    protected static byte[] calcMinHash(long[] minHashValues, int hashBit) {
        boolean shift = true;
        boolean radix = true;
        long mask = 1L;
        int pos = 0;
        int nbits = minHashValues.length * hashBit;
        FastBitSet bitSet = new FastBitSet(nbits);
        long[] var9 = minHashValues;
        int var10 = minHashValues.length;

        for (int var11 = 0; var11 < var10; ++var11) {
            long i = var9[var11];

            for (int j = 0; j < hashBit; ++j) {
                bitSet.set(pos, (int) (i & 1L) == 1);
                ++pos;
                i >>>= 1;
            }
        }

        return bitSet.toByteArray();
    }

    public final boolean incrementToken() throws IOException {
        int funcSize = this.hashFunctions.length;

        while (this.input.incrementToken()) {
            String term = this.termAttr.toString();

            for (int i = 0; i < funcSize; ++i) {
                HashCode hashCode = this.hashFunctions[i].hashUnencodedChars(term);
                long value = hashCode.asLong();
                if (value < this.minHashValues[i]) {
                    this.minHashValues[i] = value;
                }
            }
        }

        if (this.minHash != null) {
            return false;
        } else {
            this.minHash = BaseEncoding.base64().encode(calcMinHash(this.minHashValues, this.hashBit));
            this.termAttr.setEmpty().append(this.minHash);
            this.posIncrAttr.setPositionIncrement(0);
            this.offsetAttr.setOffset(0, this.minHash.length());
            return true;
        }
    }

    public void reset() throws IOException {
        super.reset();
        Arrays.fill(this.minHashValues, 9223372036854775807L);
        this.minHash = null;
    }
}
