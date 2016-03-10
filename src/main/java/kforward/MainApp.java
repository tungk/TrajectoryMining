package kforward;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
    public static void main(String[] args) {
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
   	KFL.runLogic();
   	context.close();
       }
}
