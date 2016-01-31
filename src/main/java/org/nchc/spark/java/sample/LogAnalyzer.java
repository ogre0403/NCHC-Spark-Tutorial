package org.nchc.spark.java.sample;


import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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

        JavaRDD<ApacheAccessLog> accessLogs = logLines
                .map(new Function<String, ApacheAccessLog>() {
                    @Override
                    public ApacheAccessLog call(String v1) throws Exception {
                        return parseFromLogLine(v1);
                    }
                })
                .filter(new Function<ApacheAccessLog, Boolean>() {
                    @Override
                    public Boolean call(ApacheAccessLog v1) throws Exception {
                        return v1 != null;
                    }
                })
                .cache();
        long validLogCount = accessLogs.count();
        logger.info("Valid Apache Log Count : " + validLogCount);

        // Use DoubleRDD to calculate statistics based on the content size.
        // Note how the contentSizes are cached as well since multiple actions
        //   are called on that RDD.

        JavaDoubleRDD contentSizes = accessLogs.mapToDouble(
                new DoubleFunction<ApacheAccessLog>() {
                    @Override
                    public double call(ApacheAccessLog apacheAccessLog) throws Exception {
                        return apacheAccessLog.getContentSizeDouble();
                    }
                }).cache();
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
        Map<Integer, Object>  responseCodeToCount = accessLogs
                .mapToPair(new PairFunction<ApacheAccessLog, Integer, ApacheAccessLog>() {
                    @Override
                    public Tuple2<Integer, ApacheAccessLog> call(ApacheAccessLog log) throws Exception {
                        return new Tuple2<Integer, ApacheAccessLog>(log.getResponseCode(), log);
                    }
                })
                .countByKey();
        logger.info("===============================");
        logger.info("Response code counts: ");
        for (Map.Entry<Integer, Object> entry : responseCodeToCount.entrySet()){
            logger.info(entry.getKey() + " / " + entry.getValue());
        }



        // Find out Any IPAddress that has accessed the server more than 10 times.
        logger.info("===============================");
        logger.info("IPAddress that access more than 10 times: ");
        accessLogs.mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
                .reduceByKey(SUM_REDUCER)
                .filter(tuple -> tuple._2() > ACCESS_THRESHOLD)
                .map(new Function<Tuple2<String,Long>, String>() {
                    @Override
                    public String call(Tuple2<String, Long> v1) throws Exception {
                        return v1._1();
                    }
                })
                .foreach(s -> logger.info(s));


        // Top Endpoints.
        logger.info("===============================");
        logger.info("Top 3 Endpoints: ");
        List<Tuple2<String, Long>> topEndpoints = accessLogs
                .mapToPair(log -> new Tuple2<>(log.getEndpoint(), 1L))
                .groupByKey()
                .mapValues(new Function<Iterable<Long>, Long>() {
                    @Override
                    public Long call(Iterable<Long> v1) throws Exception {
                        long sum = 0;
                        for(Long v : v1)
                            sum = sum + v;
                        return new Long(sum);
                    }
                })
                .top(3, new ValueComparator<>(Comparator.<Long>naturalOrder()));
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
