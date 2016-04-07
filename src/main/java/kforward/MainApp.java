package kforward;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


import model.SnapshotClusters;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import cluster.BasicClustering;
import cluster.ClusteringMethod;

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
	
   	int K = Integer.parseInt(AppProperties.getProperty("K"));
	int L = Integer.parseInt(AppProperties.getProperty("L"));
	int M = Integer.parseInt(AppProperties.getProperty("M"));
	int G = Integer.parseInt(AppProperties.getProperty("G"));
   	
   	JavaRDD<String> input = context.textFile(hdfs_input, hdfs_read_partitions);
   	ClusteringMethod cm = new BasicClustering();
   	JavaRDD<SnapshotClusters> CLUSTERS = cm.doClustering(input, M, Integer.parseInt(AppProperties
   		.getProperty("eps")), Integer.parseInt(AppProperties
   	   		.getProperty("minpts")), Integer.parseInt(AppProperties
   		.getProperty("snapshot_partitions")));
   	KForwardLayout KFL = new KForwardLayout(K,L,M,G);
	KFL.setInput(CLUSTERS);
   	JavaPairRDD<Integer, ArrayList<HashSet<Integer>>> result = KFL.runLogic();
   	
   	//output the result
   	List<Tuple2<Integer, ArrayList<HashSet<Integer>>>> rs = result.collect();
   	for(Tuple2<Integer, ArrayList<HashSet<Integer>>> r : rs) {
   	    if(r._2.size() != 0) {
   		for(HashSet<Integer> cluster : r._2) {
   		    System.out.println(r._1 + "\t"+ cluster);
   		}
   	    }
   	}
   	context.close();
       }
}
