package org.nchc.spark.java.exercise.i;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * Created by ogre0403 on 2016/1/23.
 */
public class HelloWorldLambda {
    private static Logger logger = Logger.getLogger(HelloWorldLambda.class);
    public static void main(String[] args) {
        String inputFile = args[0];
        SparkConf conf = new SparkConf().setAppName("HelloWorld").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(inputFile);

        JavaRDD<String> pythonLines = lines.filter(
            line -> true
            //TODO: Exercise i-3, i-4
            // filter out line containing "Python"
            // delete line -> true and replaced by appropriate Function expression
        );

        List<String> result = pythonLines.collect();
        for(String s: result)
            logger.info(s);

    }
}
