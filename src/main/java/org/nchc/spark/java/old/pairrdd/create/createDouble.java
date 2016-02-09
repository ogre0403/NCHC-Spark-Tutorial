package org.nchc.spark.java.old.pairrdd.create;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;

import java.util.Arrays;

/**
 * Created by ogre on 2015/4/26.
 */
public class createDouble {
    private static Logger logger = Logger.getLogger(createDouble.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("createDouble");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));

        //rdd.mean();

        JavaDoubleRDD result =rdd.mapToDouble(
                new DoubleFunction<Integer>() {
                    @Override
                    public double call(Integer integer) throws Exception {
                        return (double) integer;
                    }
                }
        );

        logger.info(result.mean());
    }
}
