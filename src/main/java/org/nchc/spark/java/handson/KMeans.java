package org.nchc.spark.java.handson;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.Vector;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class KMeans {
    private static Logger logger = Logger.getLogger(KMeans.class);

    static Integer closestPoint(Vector p, List<Vector> centers) {
        int bestIndex = 0;
        double closest = Double.POSITIVE_INFINITY;
        for (int i = 0; i < centers.size(); i++) {
            double tempDist = p.squaredDist(centers.get(i));
            if (tempDist < closest) {
                closest = tempDist;
                bestIndex = i;
            }
        }
        return bestIndex;
    }


    public static void main(String[] args) throws Exception {

        JavaSparkContext sc = new JavaSparkContext("local", "KMeans");
        int K = 2;
        double convergeDist = .000001;

        JavaRDD<Vector> data1 =
            sc.parallelize(
                Arrays.asList(
                    new Vector(new double[]{1.0,2.0}),
                    new Vector(new double[]{16.0,3.0}),
                    new Vector(new double[]{3.0,3.0}),
                    new Vector(new double[]{2.0,2.0}),
                    new Vector(new double[]{2.0,3.0}),
                    new Vector(new double[]{25.0,1.0}),
                    new Vector(new double[]{7.0,6.0}),
                    new Vector(new double[]{6.0,5.0}),
                    new Vector(new double[]{-1.0,-23.0})
                ));
        long count = data1.count();
        logger.info("Number of records " + count);

        List<Vector> centroids = data1.takeSample(false, K, 0);

        double tempDist;
        int itr_count =0;
        do {
            itr_count++;
            logger.info("centroids in iteration " + itr_count);
            for(Vector t: centroids)
                logger.info(t);


            JavaPairRDD<Integer, Vector> result1 = data1.mapToPair(null);
            // TODO: 分群, 計算每個Vector屬於那一個組中心
            //delete null and replaced by appropriate Function expression

            JavaPairRDD<Integer, TotalVector> result2 = result1.mapValues(null);
            // TODO: 每個Vector轉成TotalVector方便做reduceByKey
            //delete null and replaced by appropriate Function expression

            JavaPairRDD<Integer, TotalVector> result3 = result2.reduceByKey(null);
            // TODO: 將所有點的座標加總
            //delete null and replaced by appropriate Function expression

            JavaPairRDD<Integer, Vector> result4 = result3.mapValues(null);
            // TODO: 計算所有點的幾何中心, 找出新的centroid 座標
            //delete null and replaced by appropriate Function expression

            Map<Integer, Vector> newCentroids = result4.cache().collectAsMap();


            // 求新舊centroid的delta
            tempDist = 0.0;
            for (int i = 0; i < K; i++) {
                tempDist += centroids.get(i).squaredDist(newCentroids.get(i));
            }
            // 重新設定centroid
            for (Map.Entry<Integer, Vector> t: newCentroids.entrySet()) {
                centroids.set(t.getKey(), t.getValue());
            }
            logger.info("Finished iteration" + itr_count+" (delta = " + tempDist + ")");
        } while (tempDist > convergeDist);


        logger.info("Cluster with some articles:");
        for (int i = 0; i < centroids.size(); i++) {
            final int index = i;
            List<Vector> samples =
                    data1.filter(v1 -> closestPoint(v1, centroids) == index).collect();
            logger.info("Group " + i);
            for(Vector sample: samples) {
                logger.info(sample);
            }
        }
        sc.stop();
    }
}

class TotalVector implements Serializable{
    Vector vector_sum;
    int num;


    public TotalVector(double[] d, int i){
        this.vector_sum = new Vector(d);
        num = i;
    }

    public Vector getVector(){
        return vector_sum;
    }

    public TotalVector add(TotalVector v) {
        vector_sum.addInPlace(v.getVector());
        num++;
        return this;
    }

    public Vector mean(){

        return vector_sum.divide(num);
    }

    public String toString(){
        return "[" + vector_sum + "|" + num +"]";
    }
}