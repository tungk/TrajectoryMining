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
	int K = Integer.parseInt(AppProperties.getProperty("K"));
	int L = Integer.parseInt(AppProperties.getProperty("L"));
	int M = Integer.parseInt(AppProperties.getProperty("M"));
	int G = Integer.parseInt(AppProperties.getProperty("G"));
	String hdfs_input = AppProperties.getProperty("hdfs_input");
	int hdfs_read_partitions = Integer.parseInt(AppProperties
		.getProperty("hdfs_read_partitions"));

	// if (!conf.contains("spark.app.name")) {
	String name = AppProperties.getProperty("appName");
	name = name + "-K" + K + "-L" + L + "-M" + M + "-G" + G + "-Par"
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
	JavaRDD<SnapshotClusters> CLUSTERS = cm.doClustering(input,
		Integer.parseInt(AppProperties.getProperty("eps")),
		Integer.parseInt(AppProperties.getProperty("minpts")),
		M, 
		Integer.parseInt(AppProperties.getProperty("snapshot_partitions")));
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
