package org.nchc.spark.java.exercise.iv;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.nchc.spark.java.sample.ApacheAccessLog;


// To feed the new lines of some logfile into a socket, run this command:
// data/loggen.sh

public class Stateless {

    private static final Duration BATCH_INTERVAL = Durations.seconds(2);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Stateless_Transformation");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_INTERVAL);

        // Create a DStream that will connect to hostname:port, like localhost:5555
        JavaReceiverInputDStream<String> logDataDStream = jssc.socketTextStream("localhost", 5555);

        // Basic Stateless transformation
        JavaDStream<ApacheAccessLog> accessLogDStream = logDataDStream
                .map(s -> ApacheAccessLog.parseFromLogLine(s));

        //TODO: exercise iv-1
        JavaDStream<ApacheAccessLog> largerLog = accessLogDStream
                .filter(s -> s != null)
                .filter(null)
                .cache();
        largerLog.print();

        //TODO: exercise iv-1
        JavaDStream<Long> contentSizeDStream = largerLog
                .map(log -> log.getContentSize())
                .reduce(null);
        contentSizeDStream.print();

        // Start the streaming server.
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate

    }

}
