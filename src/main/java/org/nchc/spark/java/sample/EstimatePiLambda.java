package org.nchc.spark.java.sample;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class EstimatePiLambda {
    private static Logger logger = Logger.getLogger(EstimatePiLambda.class);

    public static void main(String[] args) {

        int NUM_SAMPLES = 10000;
        JavaSparkContext sc = new JavaSparkContext("local", "Pi");
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        long count = sc.parallelize(l)
                .mapToPair( v -> new Tuple2<>(Math.random(),Math.random()))
                .filter( v -> v._1()* v._1() + v._2() * v._2() < 1)
                .count();

        logger.info("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
    }
}
