package org.nchc.spark.java.sample.iv;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.nchc.spark.java.sample.ApacheAccessLog;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;

/**
 * Created by ogre0403 on 2016/2/14.
 */
public class Stateful_3 {

    // Stats will be computed for the last window length of time.
    private static final Duration WINDOW_LENGTH = Durations.seconds(6);
    // Stats will be computed every slide interval time.
    private static final Duration SLIDE_INTERVAL = Durations.seconds(4);
    private static final Duration BATCH_INTERVAL = Durations.seconds(2);


    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Stateful_Transformation");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_INTERVAL);

        // Create a DStream that will connect to hostname:port, like localhost:5555
        JavaReceiverInputDStream<String> logDataDStream = jssc.socketTextStream("localhost", 5555);

        JavaDStream<ApacheAccessLog> accessLogDStream =
                logDataDStream.map(s -> ApacheAccessLog.parseFromLogLine(s)).filter(s -> s!=null);


        // window transformation
        JavaDStream<ApacheAccessLog> largeLogWindow = accessLogDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL);
        largeLogWindow.count().print();

        largeLogWindow.foreachRDD(accessLogs -> {
            if (accessLogs.count() == 0) {
                System.out.println("No access logs in this time interval");
                return null;
            }

            // *** Note that this is code copied verbatim from LogAnalyzer.java.

            // Calculate statistics based on the content size.
            JavaRDD<Long> contentSizes =
                    accessLogs.map(ApacheAccessLog::getContentSize).cache();
            System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
                    contentSizes.reduce((a, b) -> a + b) / contentSizes.count(),
                    contentSizes.min(Comparator.naturalOrder()),
                    contentSizes.max(Comparator.naturalOrder())));

            // Compute Response Code to Count.
            List<Tuple2<Integer, Long>> responseCodeToCount =
                    accessLogs.mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1L))
                            .reduceByKey((a,b)->a+b)
                            .take(100);
            System.out.println(String.format("Response code counts: %s", responseCodeToCount));

            // Any IPAddress that has accessed the server more than 10 times.
            List<String> ipAddresses =
                    accessLogs.mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
                            .reduceByKey((a,b)->a+b)
                            .filter(tuple -> tuple._2() > 8)
                            .map(Tuple2::_1)
                            .take(100);
            System.out.println(String.format("IPAddresses > 10 times: %s", ipAddresses));

            return null;
        });

        // Start the streaming server.
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate

    }
}
