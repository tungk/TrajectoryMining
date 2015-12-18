package baselineTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import olpartitioned.BaselineLayout;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import conf.AppProperties;

public class MultiJoinTest {

    public static void main(String[] args) {
	SparkConf conf = new SparkConf();
	if (!conf.contains("spark.app.name")) {
	    conf = conf.setAppName(AppProperties.getProperty("appName"));
	}
	if (!conf.contains("spark.master")) {
	    conf = conf.setMaster(AppProperties.getProperty("spark_master"));
	}
	JavaSparkContext context = new JavaSparkContext(conf);
//	int hdfs_read_partitions = Integer.parseInt(AppProperties
//		.getProperty("hdfs_read_partitions"));
//	String hdfs_input = AppProperties.getProperty("hdfs_input");
//	BaselineLayout bl = new BaselineLayout(context.textFile(hdfs_input, hdfs_read_partitions));
//	bl.runLogic();
//	context.close();
	int input_size = 16777216;
//	int input_size = 2048;
	ArrayList<Long> input = new ArrayList<Long>();
	Random r = new Random();
	for(int i = 1; i <= input_size; i++) {
	    input.add((long) r.nextInt(i));
	}
	
	JavaRDD<Long> inputRDD = context.parallelize(input,2048);
	JavaPairRDD<Long, Long> pairedRDD = inputRDD.mapToPair(new PairFunction<Long, Long, Long>(){
	    private static final long serialVersionUID = 5099780243240918257L;
		@Override
		public Tuple2<Long, Long> call(Long t) throws Exception {
		    return new Tuple2<Long, Long>(t,t);
		}
	    });
	
	long count = pairedRDD.count();
	while(count > 1) {
	    pairedRDD = pairedRDD.mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>(){
		private static final long serialVersionUID = -1023404534179685477L;
		@Override
		public Tuple2<Long, Long> call(Tuple2<Long, Long> t)
			throws Exception {
		    return new Tuple2<Long,Long>(t._1/2, t._2);
		}
	    }).reduceByKey(new Function2<Long,Long,Long>(){
		private static final long serialVersionUID = 6706722979754573434L;
		@Override
		public Long call(Long v1, Long v2) throws Exception {
		    return v1+v2;
		}} , (int) count);
	    count = pairedRDD.count();
	}
	for(Tuple2<Long,Long> t : pairedRDD.collect() ) {
	    System.out.println(t);
	}
	context.close();
    }
}
