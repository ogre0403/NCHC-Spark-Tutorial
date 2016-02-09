package org.nchc.spark.java.old.basicrdd.transform;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by ogre on 2015/4/26.
 */
public class RDDunion {
    private static Logger logger = Logger.getLogger(RDDunion.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDDunion");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5));
        JavaRDD<Integer> result = rdd1.union(rdd2);

        logger.info(StringUtils.join(result.collect(), ","));

    }
}
