package org.nchc.spark.java.io;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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
public class RWSeq {
    private static Logger logger = Logger.getLogger(RWSeq.class);
    public static void main(String[] args) {
        String output = args[0];
        String seqOutput = output+".seq";
        String formatOutput = output+".format";
        SparkConf conf = new SparkConf().setAppName("SaveText");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String,Integer> prdd =  sc.parallelizePairs(
                Arrays.asList(
                        new Tuple2<String, Integer>("a", 1),
                        new Tuple2<String, Integer>("b", 2)
                ));


        JavaPairRDD<Text,IntWritable> result = prdd.mapToPair(new ConvertToWritableTypes());
        result.saveAsNewAPIHadoopFile(seqOutput,Text.class,IntWritable.class, SequenceFileOutputFormat.class);
        result.saveAsNewAPIHadoopFile(formatOutput,Text.class,IntWritable.class, TextOutputFormat.class);


        JavaPairRDD<Text, IntWritable> input;
        JavaPairRDD<String, Integer> readback;

        input = sc.sequenceFile(seqOutput+"/part-r-00000", Text.class, IntWritable.class);
        readback = input.mapToPair(new ConvertToNativeTypes());

        logger.info("read from sequence file : " + StringUtils.join(readback.collect(), ","));

/* maybe work on Hadoop cluster
        input = sc.newAPIHadoopFile(formatOutput+"/part-r-00000",FileInputFormat.class,Text.class,IntWritable.class,new Configuration());
        readback = input.mapToPair(new ConvertToNativeTypes());
        logger.info("read from input format file" + StringUtils.join(readback.collect(), ","));
*/
    }
}

class ConvertToWritableTypes implements
        PairFunction<Tuple2<String, Integer>, Text, IntWritable> {
    public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) {
        return new Tuple2(new Text(record._1()), new IntWritable(record._2()));
    }
}

class ConvertToNativeTypes  implements
        PairFunction<Tuple2<Text, IntWritable>, String, Integer> {
    public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record) {
        return new Tuple2(record._1().toString(), record._2().get());
    }
}