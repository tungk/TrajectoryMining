package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;
import model.SnapshotClusters;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import cluster.BasicClustering;
import cluster.ClusteringMethod;
import conf.AppProperties;
import conf.Constants;

/**
 * the entry point of apriori method.
 * Aprioir method utilizes the object partition for 
 * parallelizing trajectory pattern mining, which
 * would support SWARM patterns.
 * @author a0048267
 *
 */
public class MainApp {
    public static void main(String[] args) {
	//load with default values, and pass-in values from here
	//the conf.constants should not be used at now
	int K = 40, L = 10, M = 10, G = 3;
	int hdfs_partitions = 195;
	int eps = 15, minpt = 10 ;
	int clique_miner_partitions = 1170;
	int snapshot_partitions = hdfs_partitions;
	
	if(args.length > 0) {
	    for(String arg : args) {
		System.out.println(arg);
		if(arg.startsWith("k=") || arg.startsWith("K=")) {
		    K = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("l=")|| arg.startsWith("L=")) {
		    L = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("m=")|| arg.startsWith("M=")) {
		    M = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("g=")|| arg.startsWith("G=")) {
		    G = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("h=")|| arg.startsWith("H=")) {
		    hdfs_partitions = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("e=")|| arg.startsWith("e=")) {
		    eps = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("p=")|| arg.startsWith("p=")) {
		    minpt = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("c=")|| arg.startsWith("C=")) {
		    clique_miner_partitions = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("s=")|| arg.startsWith("S=")) {
		    snapshot_partitions = Integer.parseInt(arg.split("=")[1]);
		}
	    }
	} else {
	    System.out.println("No commandline arguments found. Using default values instead");
	    System.out.println("Usage: .bin/spark-submit --class apriori.MainApp ~/TrajectoryMining/TrajectoryMining-0.0.1-SNAPSHOT-jar-with-dependencies.jar " +
	    		"k=40 l=10 g=3 h=195 e=15 p=10 c=1170 s=195");
	    System.out.println("Missing values are replaced by defaults!");
	    System.exit(-1);
	}
	
	String hdfs_input = AppProperties.getProperty("hdfs_input");
	String name = AppProperties.getProperty("appName");
	name = name + "Apriori-K" + K + "-L" + L + "-M" + M + "-G" + G + "-Par"
		+ hdfs_partitions + "MinP" + Constants.MINPTS + "EPS"
		+ Constants.EPS;
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("aka").setLevel(Level.OFF);
	SparkConf conf = new SparkConf().setAppName(name);
	JavaSparkContext context = new JavaSparkContext(conf);
	JavaRDD<String> input = context.textFile(hdfs_input,
		hdfs_partitions);
	ClusteringMethod cm = new BasicClustering();
	JavaRDD<SnapshotClusters> CLUSTERS = cm.doClustering(input, eps, minpt, M, snapshot_partitions);
	
	//start from CLUSTERS, go into apriori
	AprioriLayout al = new AprioriLayout(K, M, L, G, clique_miner_partitions);
	al.setInput(CLUSTERS);
	//starting apriori
	JavaRDD<Iterable<IntSet>> output = al.runLogic();
	
	for(Iterable<IntSet> each_output : output.collect()) {
	    for(IntSet sbs : each_output) {
		System.out.println(sbs);
	    }
	}
	context.close();
    }
}
