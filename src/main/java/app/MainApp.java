package app;

import java.util.ArrayList;
import java.util.Iterator;

import model.Cluster;
import model.Point;
import model.SnapShot;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import conf.AppProperties;
import cluster.DBSCANClustering;
import scala.Tuple2;

public class MainApp {
   
    public static void main(String[] args) {
	String configPath;
	SparkConf conf = new SparkConf();
	if (args.length == 0) {
	    configPath = "app-config.xml";
	} else {
	    configPath = args[0];
	}
	AppProperties.initProperty(configPath);
	if (!conf.contains("spark.app.name")) {
	    conf = conf.setAppName(AppProperties.getProperty("appName"));
	}
	if (!conf.contains("spark.master")) {
	    conf = conf.setMaster(AppProperties.getProperty("spark_master"));
	}
	JavaSparkContext context = new JavaSparkContext(conf);
	JavaRDD<String> rawFiles = context.textFile(AppProperties
		.getProperty("hdfs_input"), Integer.parseInt(AppProperties
		.getProperty("hdfs_read_partitions")));
	
	
	JavaPairRDD<Integer, SnapShot> snapshots = rawFiles
		.filter(removeInvalidTuple)
		.mapToPair(tupleToSP)
		.reduceByKey(
			combineSP,
			Integer.parseInt(AppProperties
				.getProperty("snapshot_partitions")));
	
	// then for each snapshots, we need a DBSCAN
	// afterwards, snapshots contains many ArrayList of clusters. Each
	// ArrayList represent a snapshot.
	JavaRDD<ArrayList<Cluster>> clusters = snapshots.map(DBSCAN);
	
	//Each object has its cluster_id at each valid time-sequence
	//object with length length < K is filtered first
	clusters.mapToPair(null).groupByKey().filter(null);
	//then we need to find patterns among those object
	//the object growth algorithm plays a role now
	
	clusters.collect();
	clusters.saveAsTextFile(AppProperties.getProperty("hdfs_output"));
	context.close();
    }

    private static final Function<String, Boolean> removeInvalidTuple = new Function<String, Boolean>() {
	private static final long serialVersionUID = 387051918037129559L;

	@Override
	public Boolean call(String v1) throws Exception {
	    if (v1.isEmpty() || v1.charAt(0) == '#') {
		return false;
	    } else {
		return true;
	    }
	}

    };

    /**
     * this function takes in a tuple in the HDFS, and outputs a <ts, SnapShot>
     * pair
     */
    public static PairFunction<String, Integer, SnapShot> tupleToSP = new PairFunction<String, Integer, SnapShot>() {
	private static final long serialVersionUID = 4191365879318253686L;

	@Override
	public Tuple2<Integer, SnapShot> call(String t) throws Exception {
	    String[] splits = t.split("\t");
	    int obj_key = Integer.parseInt(splits[0]);
	    int ts = Integer.parseInt(splits[3]);
	    Point p = new Point(Double.parseDouble(splits[1]),
		    Double.parseDouble(splits[2]));
	    SnapShot sp = new SnapShot(ts);
	    sp.addObject(obj_key, p);
	    return new Tuple2<Integer, SnapShot>(ts, sp);
	}
    };

    /**
     * this function combines snapshots at the same point, creating real
     * snapshot
     */
    private static final Function2<SnapShot, SnapShot, SnapShot> combineSP = new Function2<SnapShot, SnapShot, SnapShot>() {
	private static final long serialVersionUID = 8610222452685128407L;

	@Override
	public SnapShot call(SnapShot v1, SnapShot v2) throws Exception {
	    v1.MergeWith(v2);
	    return v1;
	}
    };
    private static final Function<Tuple2<Integer, SnapShot>, ArrayList<Cluster>> DBSCAN = new Function<Tuple2<Integer, SnapShot>, ArrayList<Cluster>>() {
	private static final long serialVersionUID = -8210452935171979228L;
	
	@Override
	public ArrayList<Cluster> call(Tuple2<Integer, SnapShot> v1)
		throws Exception {
	    String cluster_id_prefix = SparkEnv.get().executorId();
	    DBSCANClustering dbc = new DBSCANClustering(v1._2);
	    ArrayList<Cluster> results = dbc.cluster();
	    for(int i = 0; i < results.size(); i++) {
		results.get(i).setID(Integer.parseInt(cluster_id_prefix + "00" +i));
	    }
	    return dbc.cluster();
	}
    };
    
    /**
     * remove the object cluster with size less than $K$
     */
    private static final Function<ArrayList<Cluster>, Boolean> REMOVESHORTOBJECT = new Function<ArrayList<Cluster>, Boolean>(){
	private static final long serialVersionUID = 3748376279218746194L;
	
	@Override
	public Boolean call(ArrayList<Cluster> arg0) throws Exception {
	    return null;
	}
    };

}
