package sparkexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Averaging in spark
 */
public class Average {
  private static final String APPNAME = "Average";

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("Usage: Average infile1 [infile2 ...]");
      return;
    }

    SparkConf conf = new SparkConf().setMaster("local")
        .setAppName(APPNAME);
    JavaSparkContext sc = new JavaSparkContext(conf);

    for (int i = 0; i < args.length; ++i) {
      String inputFile = args[i];
      JavaRDD<String> input = sc.textFile(inputFile);

      JavaRDD<Integer> values =
          input
              .flatMap(
                (String x) -> Arrays.asList(x.split(" "))
              ).map(Integer::parseInt);

      Tuple2<Integer, Integer> avg =
          values.aggregate(new Tuple2<>(0, 0),
              (Tuple2<Integer, Integer> acc, Integer value) ->
                  new Tuple2<>(acc._1() + value, acc._2() + 1),
              (Tuple2<Integer, Integer> acc1,
                  Tuple2<Integer, Integer> acc2) ->
                  new Tuple2<>(acc1._1() + acc2._1(), acc1._2() + acc2._2()));

      System.out.println("***********************");
      System.out.println(String.format("For file %s average is %.3f",
          inputFile, (double) avg._1() / avg._2()));
      System.out.println("***********************");

    }
  }
}
