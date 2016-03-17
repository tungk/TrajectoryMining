package kforward;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import conf.AppProperties;

/**
 * This package testifies a k-forward approach for trajectory mining
 * 
 * In this method, every local clusters copy its information to its previous $2K$ neighbors.
 * Then each cluster locally performs a CMC method to find the candidate clusters. 
 * The final candidate clusters are grouped to remove duplicates, also the inclusion-exclusion relationships.
 * 
 * @author fanqi
 *
 */
public class MainApp {
    public static void main(String[] args) throws IOException {
   	SparkConf conf = new SparkConf();
   	if (!conf.contains("spark.app.name")) {
   	    conf = conf.setAppName(AppProperties.getProperty("appName"));
   	}
   	if (!conf.contains("spark.master")) {
   	    conf = conf.setMaster(AppProperties.getProperty("spark_master"));
   	}
   	Logger.getLogger("org").setLevel(Level.OFF);
   	Logger.getLogger("aka").setLevel(Level.OFF);
   	
   	JavaSparkContext context = new JavaSparkContext(conf);
   	String hdfs_input = AppProperties.getProperty("hdfs_input");
   	int hdfs_read_partitions = Integer.parseInt(AppProperties
   		.getProperty("hdfs_read_partitions"));
   	JavaRDD<String> input = context.textFile(hdfs_input, hdfs_read_partitions);
   	KForwordLayout KFL = new KForwordLayout(input);
   	JavaPairRDD<Integer, Iterable<HashSet<Integer>>> result = KFL.runLogic();
//   	result.saveAsTextFile(AppProperties.getProperty("local_output_dir"));
   	List<Tuple2<Integer, Iterable<HashSet<Integer>>>> r = result.collect();
   	String local_output = AppProperties.getProperty("local_output_dir");
   	System.out.println(local_output);
   	FileWriter fw = new FileWriter(local_output);
   	BufferedWriter bw = new BufferedWriter(fw);
   	for(Tuple2<Integer, Iterable<HashSet<Integer>>> tuple : r) {
   	    bw.write(String.format("[%d]:%s\n",  tuple._1, tuple._2));
   	}
   	bw.close();
   	context.close();
       }
}
