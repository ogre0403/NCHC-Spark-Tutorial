package org.nchc.spark.java.basicrdd.action;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by ogre on 2015/4/26.
 */
public class RDDaggregate {
    private static Logger logger = Logger.getLogger(RDDaggregate.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("RDDaggregate");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 3));

        Function2<AvgCount, Integer, AvgCount> addAndCount =
                new Function2<AvgCount, Integer, AvgCount>() {
                    public AvgCount call(AvgCount a, Integer x) {
                        a.total += x;
                        a.num += 1;
                        return a;
                    }
                };

        Function2<AvgCount, AvgCount, AvgCount> combine =
                new Function2<AvgCount, AvgCount, AvgCount>() {
                    public AvgCount call(AvgCount a, AvgCount b) {
                        a.total += b.total;
                        a.num += b.num;
                        return a;
                    }
                };

        AvgCount initial = new AvgCount(0, 0);
        AvgCount result = rdd.aggregate(initial, addAndCount, combine);
        logger.info(result.avg());


    }


}

class AvgCount implements Serializable {

    public int total;
    public int num;
    public AvgCount(int total, int num){
        this.total = total;
        this.num = num;
    }

    public double avg(){
        return total/(double)num;
    }

}
