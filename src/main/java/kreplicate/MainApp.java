package kreplicate;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.List;

import model.SnapshotClusters;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import cluster.BasicClustering;
import cluster.ClusteringMethod;

import scala.Tuple2;
import java.util.Iterator;

import conf.AppProperties;
import conf.Constants;

/**
 * This package testifies a k-forward approach for trajectory mining
 * 
 * In this method, every local clusters copy its information to its previous
 * $2K$ neighbors. Then each cluster locally performs a CMC method to find the
 * candidate clusters. The final candidate clusters are grouped to remove
 * duplicates, also the inclusion-exclusion relationships.
 * 
 * @author fanqi
 * 
 */
public class MainApp {
    public static void main(String[] args) {
	int K = 40, L = 10, M = 10, G = 3;
	int ls_partition = 486;
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
		} else if(arg.startsWith("c=")|| arg.startsWith("C=")) {
		    ls_partition = Integer.parseInt(arg.split("=")[1]);
		} 
	    }
	} else {
	    System.out.println("No commandline arguments found. Using default values instead");
	    System.out.println("Usage: .bin/spark-submit --class apriori.MainApp ~/TrajectoryMining/TrajectoryMining-0.0.1-SNAPSHOT-jar-with-dependencies.jar " +
	    		"k=40 l=10 g=3 h=195 e=15 p=10 c=115");
	    System.out.println("Missing values are replaced by defaults!");
	    System.exit(-1);
	}
	String hdfs_input = AppProperties.getProperty("hdfs_input");
	String name = "Replicate-K" + K + "-L" + L + "-M" + M + "-G" + G + "-File"+hdfs_input
		;
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("aka").setLevel(Level.OFF);

	SparkConf conf = new SparkConf().setAppName(name);
	JavaSparkContext context = new JavaSparkContext(conf);
	
	JavaRDD<SnapshotClusters> CLUSTERS = context.objectFile(hdfs_input, ls_partition);
	// --------------- the above code should be identical to different
	// algorithms
	KReplicateLayout KFL = new KReplicateLayout(K, L, M, G);
	KFL.setInput(CLUSTERS);
	JavaPairRDD<Integer, ArrayList<IntSet>> result = KFL
		.runLogic();
	List<Tuple2<Integer, ArrayList<IntSet>>> rs = result
		.collect();
	// collect ground set for further duplicate removal
	ArrayList<IntSet> ground = new ArrayList<>();
	for (Tuple2<Integer, ArrayList<IntSet>> r : rs) {
	    if (r._2.size() != 0) {
		for (IntSet cluster : r._2) {
		    ground.add(cluster);
		}
	    }
	}
	// duplicate removal
	rs = result.filter(new DuplicateClusterFilter(ground)).collect();

	for (Tuple2<Integer, ArrayList<IntSet>> r : rs) {
	    if (r._2.size() != 0) {
		for (IntSet cluster : r._2) {
		    System.out.println(r._1 + "\t" + cluster);
		}
	    }
	}
	context.close();
    }
}

class DuplicateClusterFilter implements
	Function<Tuple2<Integer, ArrayList<IntSet>>, Boolean> {
    private static final long serialVersionUID = -602382731006034424L;
    private final ArrayList<IntSet> grounds;

    public DuplicateClusterFilter(ArrayList<IntSet> ground) {
	grounds = ground;
    }

    @Override
    public Boolean call(Tuple2<Integer, ArrayList<IntSet>> v1)
	    throws Exception {
	ArrayList<IntSet> value = v1._2;
	Iterator<IntSet> value_itr = value.iterator();
	while (value_itr.hasNext()) {
	    IntSet next = value_itr.next();
	    for (IntSet gr : grounds) {
		if (gr.containsAll(next) && gr.size() > next.size()) { // ensures
								       // not
								       // removed
								       // by
								       // self
		    return false;
		}
	    }
	}
	return true;
    }
}
