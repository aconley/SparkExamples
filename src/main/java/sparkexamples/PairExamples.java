package sparkexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * Some examples in pair
 */
public class PairExamples {

  private static void reduceByKeyExample(JavaPairRDD<Character, Double> input) {

    // Annoyingly, if you just chain this onto input,
    //  it won't compile.
    JavaPairRDD<Character, Double> sumByKey =
        input.reduceByKey(
            (Double x, Double y) -> x + y
        );

    Map<Character, Double> outMap = sumByKey.collectAsMap();

    System.out.println("********* Start Reduce By Key Output *********");
    for (Map.Entry<Character, Double> entry : outMap.entrySet()) {
      System.out.println(
          String.format("%s:%.3f", entry.getKey(), entry.getValue())
      );
    }
    System.out.println("********* End Reduce By Key Output *********");
  }

  private static void groupByKeyExample(JavaPairRDD<Character, Double> input) {

    JavaPairRDD<Character, Double> averaged =
        input.groupByKey().mapValues(PairExamples::average);
    Map<Character, Double> outMap = averaged.collectAsMap();

    System.out.println("********* Start Reduce By Key Average Output *********");
    for (Map.Entry<Character, Double> entry : outMap.entrySet()) {
      System.out.println(
          String.format("%s:%.3f", entry.getKey(), entry.getValue())
      );
    }
    System.out.println("********* End Reduce By Key Average Output *********");
  }

  private static Double average(Iterable<Double> it) {
    int n = 0;
    double sum = 0.0;
    for (Double i : it) {
      sum += i;
      ++n;
    }
    if (n > 0) {
      return sum / n;
    } else {
      return Double.NaN;
    }
  }

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
            )
        ).cache();

    reduceByKeyExample(input1);

    groupByKeyExample(input1);
  }

}
