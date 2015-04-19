Spark-sample
===============
1. Scala build and run
```sh
    sbt clean package
    $SPARK_HOME/bin/spark-submit \
      --class org.nchc.spark.scala.WordCount \
      ./target/scala-2.10/spark-sample_2.10-0.0.1.jar \
      ./README.md ./scala-wordcounts
```
2.  Maven build and run
```sh
	mvn clean && mvn compile && mvn package
    $SPARK_HOME/bin/spark-submit \
      --class org.nchc.spark.java.WordCount \
      ./target/spark-sample-0.0.1.jar \
      ./README.md ./wordcounts
    $SPARK_HOME/bin/spark-submit \
      --class org.nchc.spark.java.WordCountLambda \
      ./target/spark-sample-0.0.1.jar \
      ./README.md ./lambda-wordcounts
```
