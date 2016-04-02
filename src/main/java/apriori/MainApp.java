package apriori;

import model.SnapshotClusters;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.zaxxer.sparsebits.SparseBitSet;

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
	int K = Integer.parseInt(AppProperties.getProperty("K"));
	int L = Integer.parseInt(AppProperties.getProperty("L"));
	int M = Integer.parseInt(AppProperties.getProperty("M"));
	int G = Integer.parseInt(AppProperties.getProperty("G"));
	String hdfs_input = AppProperties.getProperty("hdfs_input");
	int hdfs_read_partitions = Integer.parseInt(AppProperties
		.getProperty("hdfs_read_partitions"));

	// if (!conf.contains("spark.app.name")) {
	String name = AppProperties.getProperty("appName");
	name = name + "Apriori-K" + K + "-L" + L + "-M" + M + "-G" + G + "-Par"
		+ hdfs_read_partitions + "MinP" + Constants.MINPTS + "EPS"
		+ Constants.EPS;
	// }
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("aka").setLevel(Level.OFF);
	SparkConf conf = new SparkConf().setAppName(name);
	JavaSparkContext context = new JavaSparkContext(conf);
	JavaRDD<String> input = context.textFile(hdfs_input,
		hdfs_read_partitions);
	ClusteringMethod cm = new BasicClustering();
	JavaRDD<SnapshotClusters> CLUSTERS = cm.doClustering(input, M);
	
	//start from CLUSTERS, go into apriori
	AprioriLayout al = new AprioriLayout(K, M, L, G);
	al.setInput(CLUSTERS);
	//starting apriori
	JavaRDD<Iterable<SparseBitSet>> output = al.runLogic();
	
	for(Iterable<SparseBitSet> each_output : output.collect()) {
	    for(SparseBitSet sbs : each_output) {
		System.out.println(sbs);
	    }
	}
	context.close();
    }
}
