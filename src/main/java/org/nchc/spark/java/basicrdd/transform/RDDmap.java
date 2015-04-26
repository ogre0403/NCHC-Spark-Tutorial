package org.nchc.spark.java.basicrdd.transform;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * Created by ogre on 2015/4/26.
 */
public class RDDmap {
    private static Logger logger = Logger.getLogger(RDDmap.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDDmap");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,3));
        JavaRDD<Integer> result = rdd.map(
                new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1) throws Exception {
                        return v1+1;
                    }
                }
        );

        logger.info(StringUtils.join(result.collect(), ","));

    }
}
