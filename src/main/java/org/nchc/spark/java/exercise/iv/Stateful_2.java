package org.nchc.spark.java.exercise.iv;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.nchc.spark.java.sample.ApacheAccessLog;
import scala.Tuple2;

import java.util.List;

/**
 * Created by ogre0403 on 2016/2/14.
 */
public class Stateful_2 {


    private static final Duration BATCH_INTERVAL = Durations.seconds(2);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Stateful_Transformation");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, BATCH_INTERVAL);
        jssc.checkpoint("/tmp/statefule_2");

        // Create a DStream that will connect to hostname:port, like localhost:5555
        JavaReceiverInputDStream<String> logDataDStream = jssc.socketTextStream("localhost", 5555);
        JavaDStream<ApacheAccessLog> accessLogDStream = logDataDStream
                .map(s -> ApacheAccessLog.parseFromLogLine(s))
                .filter(s -> s != null);

        JavaPairDStream<Integer, Long> responseCodeCountPairDStream = accessLogDStream
                .mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1L));

        //TODO: exercise iv-3
        JavaPairDStream<Integer, Long> counts = responseCodeCountPairDStream
                .updateStateByKey(
                    null
                );
        counts.print();


        // Start the streaming server.
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }
}

