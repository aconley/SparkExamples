package sparkexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Averaging square roots
 */
public class AverageSquareRootDouble {
  private static final String APPNAME = "AverageSquareRootDouble";

  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setMaster("local")
        .setAppName(APPNAME);
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Integer> input =
        sc.parallelize(Arrays.<Integer>asList(1, 2, 3, 4, 5, 6, 7, 8));

    Tuple2<Integer, Double> resultTup =
        input
          .mapToDouble(
              (Integer x) -> Math.sqrt(x.doubleValue())
          )
          .aggregate(new Tuple2<>(0, 0.0),
              (Tuple2<Integer, Double> acc, Double value) ->
                  new Tuple2<>(acc._1() + 1, acc._2() + value),
              (Tuple2<Integer, Double> acc1, Tuple2<Integer, Double> acc2) ->
                  new Tuple2<>(acc1._1() + acc2._1(), acc1._2() + acc2._2())
          );

    System.out.println("***********************");
    System.out.println(String.format("Average of sqrts is %.3f should be %.3f",
        resultTup._2() / resultTup._1(), 2.03825));
    System.out.println("***********************");
  }
}
