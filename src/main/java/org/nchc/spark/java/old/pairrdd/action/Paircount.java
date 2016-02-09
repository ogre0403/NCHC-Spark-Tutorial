package org.nchc.spark.java.old.pairrdd.action;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by ogre on 2015/5/2.
 */
public class Paircount {
    private static Logger logger = Logger.getLogger(Paircount.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Pairfilter");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Integer> prdd =
                sc.parallelizePairs(
                        Arrays.asList(
                                new Tuple2<Integer, Integer>(1, 2),
                                new Tuple2<Integer, Integer>(3, 4),
                                new Tuple2<Integer, Integer>(3, 5)
                        ));
        Map<Integer,Object> result = prdd.countByKey();

        logger.info(result.get(new Integer(3)));
    }
}
