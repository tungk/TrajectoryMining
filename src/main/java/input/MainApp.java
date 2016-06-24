package input;

import model.SnapshotClusters;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import cluster.BasicClustering;
import cluster.ClusteringMethod;

import conf.AppProperties;

/**
 * reads input from HDFS and 
 * write clusters in each snapshots to HDFS
 * @author a0048267
 *
 */
public class MainApp {
    
    public static void main(String[] args) {
	int hdfs_partitions = 87;
	int eps = 15, minpt = 10 ;
	int snapshot_partitions = 87;
	int M = minpt;
	int earth = 0;
	if(args.length > 0) {
	    for(String arg : args) {
		System.out.println(arg);
		if(arg.startsWith("h=")|| arg.startsWith("H=")) {
		    hdfs_partitions = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("e=")|| arg.startsWith("E=")) {
		    eps = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("p=")|| arg.startsWith("P=")) {
		    minpt = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("s=")|| arg.startsWith("S=")) {
		    snapshot_partitions = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("r=") || arg.startsWith("R=")) {
		    earth = Integer.parseInt(arg.split("=")[1]);
		}
	    }
	} else {
	    System.out.println("No commandline arguments found. Using default values instead");
	    System.out.println("Usage: .bin/spark-submit --class input.MainApp " +
	    		"~/TrajectoryMining/TrajectoryMining-0.0.1-SNAPSHOT-jar-with-dependencies.jar " +
	    		" e=15 p=10 h=87 s=174 r=1");
	    System.out.println("Missing values are replaced by defaults!");
	    System.exit(-1);
	}
	String hdfs_input = AppProperties.getProperty("hdfs_input");
	String hdfs_output = AppProperties.getProperty("hdfs_output");
	String name = String.format("DBSCAN-E=%d-P=%d", eps, minpt);
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("aka").setLevel(Level.OFF);
	
	
	SparkConf conf = new SparkConf().setAppName(name);
	JavaSparkContext context = new JavaSparkContext(conf);
	JavaRDD<String> input = context.textFile(hdfs_input, hdfs_partitions);
	ClusteringMethod cm = new BasicClustering(eps, minpt, M, snapshot_partitions, earth);
	JavaRDD<SnapshotClusters> CLUSTERS = cm.doClustering(input);
	String hdfs_out = String.format(hdfs_output + "/clusters-e%d-p%d",
		eps, minpt);
	CLUSTERS.saveAsObjectFile(hdfs_out);
	context.close();
    }
    
}
