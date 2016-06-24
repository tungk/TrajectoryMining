package input;

import model.SimpleCluster;
import model.SnapshotClusters;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple3;

import conf.AppProperties;

public class CountClusters {
    public static void main(String[] args) {
	int snapshot_partitions = 87;
	if (args.length > 0) {
	    for (String arg : args) {
		System.out.println(arg);
		if (arg.startsWith("h=") || arg.startsWith("H=")) {
		} else if (arg.startsWith("s=") || arg.startsWith("S=")) {
		    snapshot_partitions = Integer.parseInt(arg.split("=")[1]);
		}
	    }
	} else {
	    System.exit(-1);
	}
	String hdfs_input = AppProperties.getProperty("hdfs_input");
	String name = String.format("Statistics");
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("aka").setLevel(Level.OFF);

	SparkConf conf = new SparkConf().setAppName(name);
	JavaSparkContext context = new JavaSparkContext(conf);
	JavaRDD<SnapshotClusters> CLUSTERS = context.objectFile(hdfs_input, snapshot_partitions);
	
	Tuple3<Integer,Integer,Integer> summary = CLUSTERS.map(new Function<SnapshotClusters, Tuple3<Integer,Integer,Integer>>(){
	    private static final long serialVersionUID = -7493240043647457999L;
	    @Override
	    public Tuple3<Integer,Integer,Integer> call(SnapshotClusters v1)
		    throws Exception {
		int number_of_clusters = 0;
		int size_of_clusters = 0;
		for(SimpleCluster s: v1.getClusters()) {
		    number_of_clusters++;
		    size_of_clusters += s.getSize();
		}
		return new Tuple3<Integer,Integer,Integer>(number_of_clusters, size_of_clusters,1);
	    }}).reduce(new Function2<Tuple3<Integer,Integer,Integer>, Tuple3<Integer,Integer,Integer>, Tuple3<Integer,Integer,Integer>>(){
		private static final long serialVersionUID = -8041585702783209395L;

		@Override
		public Tuple3<Integer,Integer,Integer> call(
			Tuple3<Integer,Integer,Integer> v_1,Tuple3<Integer,Integer,Integer>v_2)
			throws Exception {
		    return new Tuple3<Integer,Integer,Integer>(v_1._1() + v_2._1(), v_1._2() + v_2._2(),
			    v_1._3() + v_2._3());
		}
	    });
	
	int number_of_clusters = summary._1();
	int size_of_clusters = summary._2();
	int snapshots = summary._3();
	System.out.printf("%d snapshots, average clusters per snapshot: %5.2f, average size per cluster: %5.2f\n",
	snapshots, number_of_clusters*1.0/snapshots, size_of_clusters*1.0/number_of_clusters);
	context.close();
    }
}