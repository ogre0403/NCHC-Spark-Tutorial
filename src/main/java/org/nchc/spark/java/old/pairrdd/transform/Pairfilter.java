package org.nchc.spark.java.old.pairrdd.transform;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ogre on 2015/5/2.
 */
public class Pairfilter {
    private static Logger logger = Logger.getLogger(Pairfilter.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Pairfilter");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Integer> prdd =
                sc.parallelizePairs(
                        Arrays.asList(
                                new Tuple2<Integer, Integer>(1,2),
                                new Tuple2<Integer, Integer>(3,4),
                                new Tuple2<Integer, Integer>(3,5)
                        ));


        JavaPairRDD<Integer,Integer> result1 = prdd.filter(new Function<Tuple2<Integer, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Integer, Integer> v1) throws Exception {
                // filter key is larger than 2
                return v1._1() > 2;
            }
        });

        logger.info("key > 2 : " + StringUtils.join(result1.collect(), ","));

        JavaPairRDD<Integer, Integer> result2 = prdd.filter(new Function<Tuple2<Integer, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Integer, Integer> v1) throws Exception {
                // filter value is even
                return v1._2() % 2 == 0;
            }
        });

        logger.info("value is even : " + StringUtils.join(result2.collect(), ","));


    }
}
