package apriori;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Iterator;
import java.util.List;

import model.SimpleCluster;
import model.SnapshotClusters;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import conf.AppProperties;

public class MainSP {
    public static void main(String[] args) {
	int K = 40, L = 10, M = 10, G = 3;
	double T = 1.0;
	int O = 1;
	int clique_miner_partitions = 486;
	
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
		    clique_miner_partitions = Integer.parseInt(arg.split("=")[1]);
		} else if(arg.startsWith("t=") || arg.startsWith("T=")) {
		    T = Double.parseDouble(arg.split("=")[1]);
		} else if(arg.startsWith("o=") || arg.startsWith("O=")) {
		    O = Integer.parseInt(arg.split("=")[1]);
		}
	    }
	} else {
	    System.out.println("No commandline arguments found. Using default values instead");
	    System.out.println("Usage: .bin/spark-submit --class apriori.MainApp ~/TrajectoryMining/TrajectoryMining-0.0.1-SNAPSHOT-jar-with-dependencies.jar " +
	    		"k=40 l=10 g=3 h=195 e=15 p=10 c=115 s=115");
	    System.out.println("Missing values are replaced by defaults!");
	    System.exit(-1);
	}
	
	String hdfs_input = AppProperties.getProperty("hdfs_input");
	String name = "Apriori-K" + K + "-L" + L + "-M" + M + "-G" + G + "-File"+hdfs_input;
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("aka").setLevel(Level.OFF);
	
	SparkConf conf = new SparkConf().setAppName(name);
	JavaSparkContext context = new JavaSparkContext(conf);
	//Load input data directly from HDFS
	JavaRDD<SnapshotClusters> CLUSTERS = context.objectFile(hdfs_input, clique_miner_partitions);
	if(T != 1.0) {
	    CLUSTERS = CLUSTERS.sample(true, T);
	} else if (O != 1) {
	    CLUSTERS = CLUSTERS.map(new ObjectSampler(O));
	}
	AlgoLayout al = new AprioriWithLB(K, M, L, G, clique_miner_partitions);
//	AlgoLayout al = new AprioriWithLB(K, M, L, G, clique_miner_partitions);
	al.setInput(CLUSTERS);
//	//starting apriori
	JavaRDD<IntSet> output = al.runLogic().filter(
		new Function<IntSet,Boolean>(){
		    private static final long serialVersionUID = 1854327010963412841L;
		    @Override
		    public Boolean call(IntSet v1) throws Exception {
			return v1.size() > 0;
		    }
		}); // we do not need distinct here, since the star-based partition guranteed distinctness
	//need further removal of duplicates
	//if a pattern (1,2,3,4) is found, it may like found (2,3,4) from other machine
	List<IntSet> grounds = output.collect();
	List<IntSet> duplicate_removed = output.filter(new DuplicateClusterFilter(grounds)).collect();
	for(IntSet each_output : duplicate_removed) {
	    System.out.println(each_output);
	}
	context.close();
    }
}
class ObjectSampler implements Function<SnapshotClusters, SnapshotClusters> {
    private static final long serialVersionUID = -7131774104285521589L;
	int o;
	public ObjectSampler(int O) {
	    this.o = O;
	}
	@Override
	public SnapshotClusters call(SnapshotClusters v1)
		throws Exception {
	    for(SimpleCluster cs: v1.getClusters()) {
		IntSet objs = cs.getObjects();
		Iterator<Integer> itr =  objs.iterator();
		while(itr.hasNext()) {
		    int obj = itr.next();
		    if(obj > o) {
			itr.remove();
		    }
		}
	    }
	    return v1;
	}
   }
