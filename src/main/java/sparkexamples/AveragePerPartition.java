package sparkexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Doing the average but per partition to be more efficient
 */
public class AveragePerPartition {

  //  We could use Tuple (or Pair), but they are immutable,
  //  and it's more efficient to not be immutable.  Hence, a POJO.
  // Has to be serializable for this to work
  private static class Acc implements Serializable {
    private int sum;
    private int count;
    public Acc() {
      sum = 0;
      count = 0;
    }
    public Acc addVal(int v) {
      sum += v;
      ++count;
      return this;
    }
    public Acc addAcc(Acc other) {
      sum += other.sum;
      count += other.count;
      return this;
    }
    public double getAvg() {
      return (double) sum / count;
    }
  }

  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setMaster("local")
        .setAppName("AveragePerPartition");
    JavaSparkContext sc = new JavaSparkContext(conf);

    final int nPartitions = 5;
    final int n = 1000;

    double expected = 0.5 * (n - 1);
    ArrayList<Integer> val = new ArrayList<>(n);
    for (int i = 0; i < n; ++i) {
      val.add(i);
    }
    JavaRDD<Integer> input = sc.parallelize(val, nPartitions);

    Acc avg = input.mapPartitions(
        it ->
        {
          Acc acc = new Acc();
          while (it.hasNext())
            acc.addVal(it.next());
          return Arrays.asList(acc); // Has to return an iterable
        }
    ).reduce(
        (a, b) -> a.addAcc(b)
    );

    System.out.println("***********************");
    System.out.println(
        String.format("Average is %.3f expected %.3f",
          avg.getAvg(), expected)
      );
    System.out.println("***********************");

  }
}
