package org.nchc.spark.java.pairrdd.transform;

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
public class PairMapValue {
    private static Logger logger = Logger.getLogger(PairMapValue.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PairMapValue");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Integer> prdd =
                sc.parallelizePairs(
                        Arrays.asList(
                                new Tuple2<Integer, Integer>(1, 2),
                                new Tuple2<Integer, Integer>(3, 4),
                                new Tuple2<Integer, Integer>(3, 5)
                        ));
        
        JavaPairRDD<Integer,Integer> result =
                prdd.mapValues(new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1) throws Exception {
                    return v1 +1;
                }
        });

        logger.info(StringUtils.join(result.collect(), ","));

    }
}
