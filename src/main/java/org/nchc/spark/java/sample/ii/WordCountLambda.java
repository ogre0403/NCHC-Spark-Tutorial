package org.nchc.spark.java.sample.ii;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ogre on 2015/4/19.
 */
public class WordCountLambda {
    private static Logger logger = Logger.getLogger(WordCountLambda.class);

    public static void main(String[] args) {
        String inputFile = args[0];

        SparkConf conf = new SparkConf().setAppName("wordCountLambda").setMaster("local");
        List<Tuple2<String, Integer>> result;

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Integer> word_one =  sc.textFile(inputFile)
                .flatMap(line -> Arrays.asList(line.split(" ")))
                .mapToPair(w -> new Tuple2<>(w, 1)).cache();


        result = word_one.reduceByKey((a, b) -> a + b).collect();
        for(Tuple2 r : result)
            logger.info(r);

        result = word_one.foldByKey(0, (a,b) -> a + b).collect();
        for(Tuple2 r : result)
            logger.info(r);

        result = word_one.groupByKey().mapValues(l -> SUM(l)).collect();
        for(Tuple2 r : result)
            logger.info(r);
    }

     static int SUM(Iterable<Integer> list){
        int sum = 0;
        for (Integer v : list)
            sum = sum + v;
        return sum;
    }
}
