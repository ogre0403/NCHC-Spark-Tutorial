package org.nchc.spark.java.sample.iv;

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
import org.codehaus.janino.Java;
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
        JavaRDD<JavaPerson> sampleData = people.map(
                new Function<String, JavaPerson>() {
                    public JavaPerson call(String line) throws Exception {
                        String[] parts = line.split(",");
                        JavaPerson person = new JavaPerson(Integer.parseInt(parts[0]), parts[1], parts[2], parts[3]);

                        return person;
                    }
                });

        DataFrame dfFromBEAN = sqlContext.createDataFrame(sampleData, JavaPerson.class);
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

        JavaRDD<Row> rowRDD = people.map(
                new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        String[] fields = record.split(",");
                        return RowFactory.create(Integer.parseInt(fields[0]), fields[1], fields[2], fields[3]);
                    }
                });

        DataFrame dfFromSchema = sqlContext.createDataFrame(rowRDD, schema);

        logger.info("DataFrames create from schema");
        dfFromSchema.show(5);

    }
}
