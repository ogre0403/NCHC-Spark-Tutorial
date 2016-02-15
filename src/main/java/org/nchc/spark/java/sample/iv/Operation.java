package org.nchc.spark.java.sample.iv;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by ogre0403 on 2016/2/15.
 */
public class Operation {
    private static Logger logger = Logger.getLogger(Operation.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("CreateDataFrames").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame student_df = sqlContext.read().json("data/sample_data.json");
        DataFrame score_df = sqlContext.read().json("data/score.json");

        // DSL style
        logger.info("DSL Demo Result");
        student_df.select("id", "email").filter("id > 10").orderBy(student_df.col("email").desc()).show(7);
        student_df.join(score_df, student_df.col("id").equalTo(score_df.col("id")))
                .select(student_df.col("studentName"), score_df.col("score"))
                .where(score_df.col("score").gt(70))
                .show();

        //SQL style
        student_df.registerTempTable("students");
        score_df.registerTempTable("score");

        logger.info("SQL Demo result");
        sqlContext.sql("select id, email from students where id > 10 order by email desc").show(7);
        sqlContext.sql("select  students.studentName, score.score " +
                "from students inner join score on students.id = score.id" +
                " where score.score > 70")
                .show();
    }
}
