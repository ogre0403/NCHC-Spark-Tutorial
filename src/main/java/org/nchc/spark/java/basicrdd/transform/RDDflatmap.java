package org.nchc.spark.java.basicrdd.transform;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;

/**
 * Created by ogre on 2015/4/26.
 */
public class RDDflatmap {
    private static Logger logger = Logger.getLogger(RDDflatmap.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDDflatmap");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello word","hi"));
        JavaRDD<String> result = rdd.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String line) throws Exception {
                        return Arrays.asList(line.split(" "));
                    }
                }
        );

        logger.info(StringUtils.join(result.collect(), ","));

    }
}
