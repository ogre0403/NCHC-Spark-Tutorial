package org.nchc.spark.java.exercise.iv;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.nchc.spark.java.sample.JavaPerson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ogre0403 on 2016/2/14.
 */
public class CreateDF {
    private static Logger logger = Logger.getLogger(CreateDF.class);

    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf().setAppName("CreateDataFrames").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // Approach I
        // create DataFrmae from JSON
        DataFrame dfFromJSON = sqlContext.read().json("data/sample_data.json");
        logger.info("DataFrames create from JSON");
        dfFromJSON.show(5);

        // Approach II
        // create DataFrmae from Java Bean
        JavaRDD<String> people = sc.textFile("data/sample_data.csv");
        // TODO: Exercise iv-4
        JavaRDD<JavaPerson> sampleData = people.map(null);

        DataFrame dfFromBEAN = sqlContext.createDataFrame(sampleData, null);
        logger.info("DataFrames create from Java bean");
        dfFromBEAN.show(5);


        // Approach III
        // create DataFrame from schema
        // Generate the schema
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("studentName", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("phone", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("email", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(fields);
        // TODO: Exercise iv-4
        JavaRDD<Row> rowRDD = people.map(null);

        DataFrame dfFromSchema = sqlContext.createDataFrame(rowRDD, schema);

        logger.info("DataFrames create from schema");
        dfFromSchema.show(5);

    }
}
