package org.nchc.spark.java.handson;


import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.nchc.spark.java.sample.ApacheAccessLog;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The LogAnalyzer takes in an apache access log file and
 * computes some statistics on them.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter1.LogsAnalyzer"
 *     --master local[4]
 *     target/log-analyzer-1.0.jar
 *     ../../data/apache.accesslog
 */
public class LogAnalyzer {
    private static Logger logger = Logger.getLogger(LogAnalyzer.class);
    private static final String LOG_ENTRY_PATTERN =
            // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
            "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    private static int ACCESS_THRESHOLD = 10;

//    private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;
    private  static Function2<Long, Long, Long> SUM_REDUCER = new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1+ v2;
            }
    };

    private static class ValueComparator<K, V>
            implements Comparator<Tuple2<K, V>>, Serializable {
        private Comparator<V> comparator;

        public ValueComparator(Comparator<V> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
            return comparator.compare(o1._2(), o2._2());
        }
    }

    private static class MyComparator implements Comparator<Double>, Serializable{
        @Override
        public int compare(Double o1, Double o2) {
            return o1.compareTo(o2);
        }
    }

    public static void main(String[] args) {
        // Create a Spark Context.
        SparkConf conf = new SparkConf().setAppName("Log Analyzer").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the text file into Spark.
        if (args.length == 0) {
            System.out.println("Must specify an access logs file.");
            System.exit(-1);
        }
        String logFile = args[0];
        JavaRDD<String> logLines = sc.textFile(logFile);
        long count = logLines.count();
        logger.info("Total Apache Log Count : " + count);

        // Convert the text log lines to ApacheAccessLog objects and cache them
        //   since multiple transformations and actions will be called on that data.

        JavaRDD<ApacheAccessLog> result1 = logLines.map(null);
        //TODO: 利用parseFromLogLine()將日志字串轉成ApacheAccessLog物件

        JavaRDD<ApacheAccessLog> result2 = result1.filter(null);
        // TODO: 將不符合格式的資料篩除

        JavaRDD<ApacheAccessLog> accessLogs = result2.cache();
        long validLogCount = accessLogs.count();
        logger.info("Valid Apache Log Count : " + validLogCount);

        // Use DoubleRDD to calculate statistics based on the content size.
        // Note how the contentSizes are cached as well since multiple actions
        //   are called on that RDD.

        JavaDoubleRDD contentSizes = accessLogs.mapToDouble(null).cache();
        // TODO: 利用ApacheAccessLog的getContentSizeDouble(), 產生DoubleRDD

        logger.info("===============================");
        logger.info("Contents basic statistic: ");
        logger.info((String.format("Avg: %s, Stdev: %s, Var %s, Min: %s, Max: %s",
                contentSizes.mean(),
                contentSizes.stdev(),
                contentSizes.variance(),
                contentSizes.min(Comparator.naturalOrder()),
                contentSizes.max(new MyComparator())
        )));


        // Use countByKey to Compute Count of each Response Code.


        JavaPairRDD<Integer, ApacheAccessLog> result3 = accessLogs.mapToPair(null);
        Map<Integer, Object> responseCodeToCount = result3.countByKey();
        //TODO: 將JavaRDD<ApacheAccessLog>轉成JavaPairRDD<Integer, ApacheAccessLog>
        // 利用ApacheAccessLog的getResponseCode()取得Integer型態的http protocol回傳值


        logger.info("===============================");
        logger.info("Response code counts: ");
        for (Map.Entry<Integer, Object> entry : responseCodeToCount.entrySet()){
            logger.info(entry.getKey() + " / " + entry.getValue());
        }



        // Find out Any IPAddress that has accessed the server more than 10 times.
        logger.info("===============================");
        logger.info("IPAddress that access more than 10 times: ");
        JavaPairRDD<String, Long> result4 = accessLogs.mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L));
        JavaPairRDD<String, Long> result5 = result4.reduceByKey(SUM_REDUCER);
        JavaPairRDD<String, Long> result6 = result5.filter(null);
        //TODO: 篩選出大於threshold的ip

        JavaRDD<String> result7 = result6.map( null);
        //TODO: 利用map()將JavaPairRDD<String, Long>轉成JavaRDD<String>

        result7.foreach(null);
        // TODO: 印出RDD中的每個元素


        // Top Endpoints.
        logger.info("===============================");
        logger.info("Top 3 Endpoints: ");
        JavaPairRDD<String, Long> result8 = accessLogs.mapToPair(log -> new Tuple2<>(log.getEndpoint(), 1L));
        JavaPairRDD<String, Long> result9 = result8.groupByKey().mapValues(null);
        //TODO: 利用mapValues()計算groupByKey()結果的總合
        // 作用相當於reduceByKey()

        List<Tuple2<String, Long>> topEndpoints = result9.top(3, new ValueComparator<>(Comparator.<Long>naturalOrder()));
        logger.info(String.format("Top Endpoints: %s", topEndpoints));

        // Stop the Spark Context before exiting.
        sc.stop();
    }

    public static ApacheAccessLog parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            logger.error("Cannot parse logline : " + logline);
            return null;
        }

        return new ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
                m.group(5), m.group(6), m.group(7), m.group(8), m.group(9));
    }

}
