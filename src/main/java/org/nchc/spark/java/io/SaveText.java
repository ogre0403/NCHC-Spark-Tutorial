package org.nchc.spark.java.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ogre on 2015/5/2.
 */
public class SaveText {
    private static Logger logger = Logger.getLogger(SaveText.class);
    public static void main(String[] args) {
        String output = args[0];
        SparkConf conf = new SparkConf().setAppName("SaveText");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String,Integer> prdd =  sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<String, Integer>("a", 1),
                        new Tuple2<String, Integer>("b", 2)
                ));
//        JavaPairRDD<Text,IntWritable> result = prdd.mapToPair(new ConvertToWritableTypes());
//        result.saveAsNewAPIHadoopFile(output,IntWritable.class,Text.class, TextOutputFormat.class);
        prdd.saveAsNewAPIHadoopFile(output,String.class,Integer.class, TextOutputFormat.class);
    }
}

class ConvertToWritableTypes implements
        PairFunction<Tuple2<String, Integer>, Text, IntWritable> {
    public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) {
        return new Tuple2(new Text(record._1()), new IntWritable(record._2()));
    }
}
