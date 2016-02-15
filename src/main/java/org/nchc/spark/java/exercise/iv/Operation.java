package org.nchc.spark.java.exercise.iv;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class Operation {
    private static Logger logger = Logger.getLogger(Operation.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("CreateDataFrames").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame student_df = sqlContext.read().json("data/sample_data.json");
        DataFrame score_df = sqlContext.read().json("data/score.json");

        // DSL style
        // TODO: Exercise iv-5
        logger.info("DSL Demo Result");
        student_df.select("").filter("").orderBy().show(7);
        student_df.join(null).select("").where("").show();

        //SQL style
        student_df.registerTempTable("students");
        score_df.registerTempTable("score");

        // TODO: Exercise iv-5
        logger.info("SQL Demo result");
        sqlContext.sql("").show(7);
        sqlContext.sql("").show();
    }
}
