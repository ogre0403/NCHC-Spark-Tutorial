package org.nchc.spark.java.basicrdd.action;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

/**
 * Created by ogre on 2015/4/26.
 */
public class RDDreduce {
    private static Logger logger = Logger.getLogger(RDDreduce.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDDreduce");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,3));
        Integer result = rdd.reduce(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );
        logger.info(result.intValue());
    }

}
