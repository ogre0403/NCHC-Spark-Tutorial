package org.nchc.spark.java.handson;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * Created by ogre0403 on 2016/1/23.
 */
public class HelloWorld {
    private static Logger logger = Logger.getLogger(HelloWorld.class);
    public static void main(String[] args) {
        String inputFile = args[0];
        SparkConf conf = new SparkConf().setAppName("HelloWorld").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(inputFile);

        JavaRDD<String> pythonLines = lines.filter(
            null
            //TODO: filter out line containing "Python"
            // delete null and replaced by appropriate Function class
        );

        List<String> result = pythonLines.collect();

        for(String s: result)
            logger.info(s);
    }
}
