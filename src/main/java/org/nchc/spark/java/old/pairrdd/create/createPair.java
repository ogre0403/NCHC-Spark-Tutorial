package org.nchc.spark.java.old.pairrdd.create;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ogre on 2015/4/26.
 */
public class createPair {
    private static Logger logger = Logger.getLogger(createPair.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("createPair");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));

        JavaPairRDD<String, String> result = rdd.mapToPair(
                new PairFunction<Integer, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Integer integer) throws Exception {
                        Integer sqr = integer * integer;
                        return new Tuple2<String, String>(integer.toString(), sqr.toString());
                    }
                }
        );

        for(Tuple2<String,String> t : result.collect()) {
            logger.info(String.format("{%s,%s}", t._1(),t._2()));
        }
    }
}
