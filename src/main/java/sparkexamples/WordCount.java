package sparkexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.Arrays;

/**
 * You knew it was coming
 */
public class WordCount {
  private static final String APPNAME = "WordCount";

  public static void main(String[] args) {
    if ((args.length & 1) != 0) {
      System.out.println("Error -- need input/output file pairs");
      return;
    }

    SparkConf conf = new SparkConf().setMaster("local")
        .setAppName(APPNAME);
    JavaSparkContext sc = new JavaSparkContext(conf);

    for (int i = 0; i < args.length; i += 2) {
      String inputFile = args[i];
      String outputFile = args[i + 1];
      System.out.println("Counting words in " + inputFile
        + " outputting to " + outputFile);
      JavaRDD<String> input = sc.textFile(inputFile);
      JavaRDD<String> words =
          input.flatMap((String x) -> Arrays.asList(x.split(" ")));
      JavaPairRDD<String, Integer> counts =
          words.mapToPair((String x) -> new Tuple2<String, Integer>(x, 1))
            .reduceByKey((Integer x, Integer y) -> x + y);
      counts.saveAsTextFile(outputFile);
    }
  }
}
