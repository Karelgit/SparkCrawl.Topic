package com.gengyun.utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by hadoop on 2015/11/24.
 */
public class Convert implements Serializable {


    public PairFunction<Tuple2<Text, LongWritable>, String, Long> ConvertToNativeTypes() {
        PairFunction<Tuple2<Text, LongWritable>, String, Long> result = new PairFunction<Tuple2<Text, LongWritable>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<Text, LongWritable> record) throws Exception {
                return new Tuple2(record._1().toString(), record._2().get());
            }
        };

        return result;
    }


    public PairFunction<Tuple2<String, Long>, Text, LongWritable> ConvertToWritableTypes() {

        PairFunction<Tuple2<String, Long>, Text, LongWritable> result = new PairFunction<Tuple2<String, Long>, Text, LongWritable>() {
            @Override
            public Tuple2<Text, LongWritable> call(Tuple2<String, Long> record) throws Exception {
                return new Tuple2(new Text(record._1()), new LongWritable(record._2()));
            }
        };
        return result;
    }





}
