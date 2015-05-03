package org.nchc.spark.java.io;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by ogre on 2015/5/3.
 */
public class TextFile {
    private static Logger logger = Logger.getLogger(TextFile.class);
    public static void main(String[] args) {
        String input = args[0];
        String output = args[1];
        SparkConf conf = new SparkConf().setAppName("TextFile");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(input);
        JavaRDD<String> result = rdd.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return new Integer(Integer.parseInt(v1) +1).toString();
            }
        });

        JavaPairRDD<String, String> prdd = rdd.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                int i = Integer.parseInt(s);
                return new Tuple2(i+"", (i+1) +"");
            }
        });

        result.saveAsTextFile(output);
        prdd.saveAsTextFile(output+".pair");
    }
}
