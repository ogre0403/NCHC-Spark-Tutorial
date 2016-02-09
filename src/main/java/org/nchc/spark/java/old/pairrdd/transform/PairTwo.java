package org.nchc.spark.java.old.pairrdd.transform;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ogre on 2015/5/2.
 */
public class PairTwo {
    private static Logger logger = Logger.getLogger(PairTwo.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PairTwo");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> prdd =
                sc.parallelizePairs(
                        Arrays.asList(
                                new Tuple2<String, Integer>("panda", 0),
                                new Tuple2<String, Integer>("pink", 3),
                                new Tuple2<String, Integer>("pirate", 3),
                                new Tuple2<String, Integer>("panda", 1),
                                new Tuple2<String, Integer>("pink", 4)
                        ));

        JavaPairRDD<String,Tuple2<Integer,Integer>> result =
                prdd
                .mapValues(new Function<Integer, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Integer v1) throws Exception {
                        return new Tuple2<Integer, Integer>(v1,1);
                    }
                })
                .reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                        return new Tuple2<Integer, Integer>(v1._1()+v2._1(), v1._2()+v2._2());
                    }
                });

        logger.info(StringUtils.join(result.collect(), ","));
    }
}
