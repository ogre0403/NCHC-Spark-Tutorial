package org.nchc.spark.java.exercise;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class EstimatePi {
    private static Logger logger = Logger.getLogger(EstimatePi.class);

    public static void main(String[] args) {

        int NUM_SAMPLES = 10000;
        JavaSparkContext sc = new JavaSparkContext("local", "Pi");
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        long count = sc.parallelize(l)
                .mapToPair(null
                    //TODO: Exercise ii-6
                    // use mapToPair() to generate random axis (x,y)
                    //delete null and replaced by appropriate Function class
                )
                .filter(null
                    //TODO: Exercise ii-6
                    // use filter() to filter out (x,y) outside unit cycle
                    //delete null and replaced by appropriate Function class
                )
                .count();

        logger.info("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
    }
}
