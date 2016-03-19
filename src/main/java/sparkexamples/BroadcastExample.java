package sparkexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public class BroadcastExample {
  // Just to test that you can broadcast something from a previous step
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setMaster("local")
        .setAppName("ReduceKeyExample");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaPairRDD<Character, Double> input1 =
        sc.parallelizePairs(
            Arrays.<Tuple2<Character, Double>>asList(
                new Tuple2('a', 22.0),
                new Tuple2('b', 10.0),
                new Tuple2('c', 15.0),
                new Tuple2('b', 15.0),
                new Tuple2('b', 10.0),
                new Tuple2('a', 11.0)
            ),
            2
        );

    // Can't just do this directly to input1 or it won't compile
    Map<Character, Double> sumMap =
      input1.reduceByKey(
            (Double x, Double y) -> x + y
        ).collectAsMap();

    final Broadcast<Map<Character, Double>> bCastMap =
        sc.broadcast(sumMap);

    JavaRDD<Double> rdd =
        sc.parallelize(
            Arrays.asList('a', 'c', 'a', 'd')
        ).map(
            c -> bCastMap.value().get(c)
        );

    System.out.println("**********************");
    System.out.println(rdd.collect());
    System.out.println("**********************");
  }
}