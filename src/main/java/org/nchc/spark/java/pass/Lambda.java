package org.nchc.spark.java.pass;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;

/**
 * Created by ogre on 2015/4/22.
 */
public class Lambda {
    private static Logger logger = Logger.getLogger(Lambda.class);
    public static void main(String[] args) {
        String inputFile = args[0];
        String word = args[1];

        SparkConf conf = new SparkConf().setAppName("lambdaTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(inputFile);

        // inner anonymous class
        long c = lines.filter(
                new Function<String, Boolean>() {
                    public Boolean call(String s) {
                        return s.contains(word);
                    }
                }).count();
        logger.info("anonymous class:" + c);

        // named class
        List<String> ss = lines.filter(new Contain(word)).collect();
        for(String s : ss)
                logger.info("named class:" +s);

        // lambda express
        String r = lines.filter(s -> s.contains(word)).first();
        logger.info("lambda express: " + r);
    }

    static class Contain implements Function<String, Boolean>{
        private String query;
        public Contain(String q){
            this.query = q;
        }
        @Override
        public Boolean call(String v1) throws Exception {
            return v1.contains(query);
        }
    }

}


