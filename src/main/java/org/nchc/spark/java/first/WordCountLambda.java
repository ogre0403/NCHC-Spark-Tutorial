package org.nchc.spark.java.first;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ogre on 2015/4/19.
 */
public class WordCountLambda {
    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFile = args[1];

        SparkConf conf = new SparkConf().setAppName("wordCountLambda");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.textFile(inputFile)
                .flatMap(line -> Arrays.asList(line.split(" ")))
                .mapToPair(w -> new Tuple2<>(w, 1))
                .reduceByKey((a,b) -> a+b)
                .saveAsTextFile(outputFile);

    }
}
