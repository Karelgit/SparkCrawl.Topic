package com.gengyun.huanghai;


public class FastBitSet {
 final byte[] data;
 final int nbit;

 public FastBitSet(int nbit) {
     this.nbit = nbit;
     if(nbit == 0) {
         throw new IllegalArgumentException("nbit is above 0.");
     } else {
         this.data = new byte[(nbit - 1) / 8 + 1];
     }
 }

 public void set(int bitIndex, boolean value) {
     int bytePos = bitIndex / 8;
     int bitPos = bitIndex % 8;
     if(bytePos < this.data.length) {
         switch(bitPos) {
         case 0:
             this.data[bytePos] = (byte)(this.data[bytePos] & 254);
             if(value) {
                 this.data[bytePos] = (byte)(this.data[bytePos] | 1);
             }
             break;
         case 1:
             this.data[bytePos] = (byte)(this.data[bytePos] & 253);
             if(value) {
                 this.data[bytePos] = (byte)(this.data[bytePos] | 2);
             }
             break;
         case 2:
             this.data[bytePos] = (byte)(this.data[bytePos] & 251);
             if(value) {
                 this.data[bytePos] = (byte)(this.data[bytePos] | 4);
             }
             break;
         case 3:
             this.data[bytePos] = (byte)(this.data[bytePos] & 247);
             if(value) {
                 this.data[bytePos] = (byte)(this.data[bytePos] | 8);
             }
             break;
         case 4:
             this.data[bytePos] = (byte)(this.data[bytePos] & 239);
             if(value) {
                 this.data[bytePos] = (byte)(this.data[bytePos] | 16);
             }
             break;
         case 5:
             this.data[bytePos] = (byte)(this.data[bytePos] & 223);
             if(value) {
                 this.data[bytePos] = (byte)(this.data[bytePos] | 32);
             }
             break;
         case 6:
             this.data[bytePos] = (byte)(this.data[bytePos] & 191);
             if(value) {
                 this.data[bytePos] = (byte)(this.data[bytePos] | 64);
             }
             break;
         case 7:
             this.data[bytePos] = (byte)(this.data[bytePos] & 127);
             if(value) {
                 this.data[bytePos] = (byte)(this.data[bytePos] | 128);
             }
         }
     }

 }

 public byte[] toByteArray() {
     return this.data;
 }
}
