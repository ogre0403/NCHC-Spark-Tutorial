package org.nchc.spark.java.sample.iv;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.nchc.spark.java.sample.ApacheAccessLog;


/**
 * Created by ogre0403 on 2016/2/14.
 */
public class Stateful_1 {

    // Stats will be computed for the last window length of time.
    private static final Duration WINDOW_LENGTH = Durations.seconds(6);
    // Stats will be computed every slide interval time.
    private static final Duration SLIDE_INTERVAL = Durations.seconds(4);
    private static final Duration BATCH_INTERVAL = Durations.seconds(2);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Stateful_Transformation");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_INTERVAL);
        jssc.checkpoint("/tmp/statefule_2");

        // Create a DStream that will connect to hostname:port, like localhost:5555
        JavaReceiverInputDStream<String> logDataDStream = jssc.socketTextStream("localhost", 5555);

        JavaDStream<ApacheAccessLog> accessLogDStream = logDataDStream
                .map(s -> ApacheAccessLog.parseFromLogLine(s))
                .filter(s -> s != null)
                .filter(s -> s.getContentSize() > 5000);
        accessLogDStream.print();


        JavaDStream<Long> logCountInWindow = accessLogDStream
                .countByWindow(WINDOW_LENGTH, SLIDE_INTERVAL);
        logCountInWindow.print();

        JavaDStream<Long> contentSizeInWindow = accessLogDStream
                .map(s -> s.getContentSize()).
                reduceByWindow((Function2<Long, Long, Long>) (a, b) -> (a + b), WINDOW_LENGTH, SLIDE_INTERVAL);
        contentSizeInWindow.print();

        // Start the streaming server.
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate

    }
}
