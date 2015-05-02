package org.nchc.spark.java.bfs;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.LinkedList;

/**
 * Created by ogre on 2015/5/2.
 */
public class BFS {
    private static Logger logger = Logger.getLogger(BFS.class);
    public static void main(String[] args) {
        String inputFile = args[0];
        String output;
        SparkConf conf = new SparkConf().setAppName("BFS");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load our input data.
        int iteration = 0;
        while(iteration < 4) {
            JavaRDD<String> input;

            if (iteration == 0)
                input = sc.textFile(inputFile);
            else
                input = sc.textFile("BFS-"  +iteration+"/part-r-00000");
            iteration++;
            output = "BFS-" + iteration;

            JavaRDD<String> words = input.flatMap(
                    new FlatMapFunction<String, String>() {
                        public Iterable<String> call(String x) {
                            Node node = new Node(x);
                            LinkedList<String> ll = new LinkedList<String>();
                            if (node.getColor() == Node.Color.GRAY) {
                                for (int v : node.getEdges()) {
                                    Node vnode = new Node(v);
                                    vnode.setDistance(node.getDistance() + 1);
                                    vnode.setColor(Node.Color.GRAY);
                                    ll.add(vnode.toString());
                                }
                                node.setColor(Node.Color.BLACK);
                            }

                            ll.add(node.toString());
                            return ll;
                        }
                    });

            JavaPairRDD<Integer, Node> nnn = words.mapToPair(new PairFunction<String, Integer, Node>() {
                @Override
                public Tuple2<Integer, Node> call(String s) throws Exception {
                    Node node = new Node(s);

                    return new Tuple2<Integer, Node>(node.getId(), node);
                }
            });

            JavaPairRDD<Integer, Node> result = nnn.foldByKey(new Node(),
                    new Function2<Node, Node, Node>() {
                        @Override
                        public Node call(Node v1, Node v2) throws Exception {

                            return v1.fold(v2);
                        }
                    });

            JavaPairRDD<Integer, String> pr = result.mapToPair(new PairFunction<Tuple2<Integer, Node>, Integer, String>() {
                @Override
                public Tuple2<Integer, String> call(Tuple2<Integer, Node> x) throws Exception {
                    return new Tuple2<Integer, String>(x._1(), x._2().getLine());
                }
            });

            pr.saveAsNewAPIHadoopFile(output, Integer.class, String.class, TextOutputFormat.class);
        }
    }
}
