package org.nchc.spark.java.old.pairrdd.transform;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ogre on 2015/5/2.
 */
public class PairReduceByKey {
    private static Logger logger = Logger.getLogger(PairReduceByKey.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PairReduceByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Integer> prdd =
                sc.parallelizePairs(
                        Arrays.asList(
                                new Tuple2<Integer, Integer>(1, 2),
                                new Tuple2<Integer, Integer>(3, 4),
                                new Tuple2<Integer, Integer>(3, 5)
                        ));

        JavaPairRDD<Integer,Integer> result =
                prdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });

        logger.info(StringUtils.join(result.collect(), ","));
    }
}
