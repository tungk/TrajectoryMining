package app;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

import model.Cluster;
import model.GroupClusters;
import model.Pattern;
import model.Point;
import model.SnapShot;
import model.TemporalCluster;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import conf.AppProperties;
import cluster.DBSCANClustering;
import scala.Tuple2;

public class MainApp {
    
    private static class LOCALCMCMINING implements 
    	Function<GroupClusters, ArrayList<Pattern>> {
	private static final long serialVersionUID = 4766029572178793179L;
	private int l, g;
	public LOCALCMCMINING(int l, int g) {
	    this.l = l;
	    this.g = g;
	}

	@Override
	public ArrayList<Pattern> call(GroupClusters gc) throws Exception {
	    return null;
	}
    }
    
    /**
     * try this out...
     * @author a0048267
     *
     */
    private static class OVERLAPPARTITION implements
    	PairFlatMapFunction<ArrayList<Cluster>, Integer, ArrayList<Cluster>>{
	private static final long serialVersionUID = -8300945673433775699L;
//	private int[] schedule;
	private int start, end;
	private int l; // l is for the overlap
	private int num_of_pars, each_par_size;
	public OVERLAPPARTITION(Integer _1, Integer _12) {
	    num_of_pars = Integer.parseInt(AppProperties.getProperty("group_partition"));
	    l = Integer.parseInt(AppProperties.getProperty("L"));
	    start = _1;
	    end =_12;
	    each_par_size = (int) Math.ceil(((start-end+1)+l*(num_of_pars -1))*1.0/num_of_pars); 
//	    schedule = new GroupClusters[num_of_pars];
//	    for(int i = 0; i < num_of_pars; i++) {
//		int end = (i+1)*each_par_size - i * l - 1;
//		int start = i*(each_par_size - l);
//		schedule[i] = new GroupClusters(start, end);
//	    }
	}
	@Override
	public Iterable<Tuple2<Integer, ArrayList<Cluster>>> call(
		ArrayList<Cluster> t) throws Exception {
	    ArrayList<Tuple2<Integer, ArrayList<Cluster>>> results = new ArrayList<>();
	    int ts = t.get(0).getTS();
	    //find the corresponding group cluster, and emit group-cluster pair
	    int index = ts / (each_par_size - l);
	    int remain = ts - index * (each_par_size - l);
	    int prev_index = -1;
	    int next_index = -1;
	    if(remain < l) {
		if(index != 0) {
		    prev_index = index -1;    
		}
	    } else if (each_par_size - remain < l) {
		if(index != num_of_pars -1) {
		    next_index = index + 1;
		}
	    }
	    results.add(new Tuple2<Integer, ArrayList<Cluster>>(index, t));
	    if(prev_index != -1) {
		results.add(new Tuple2<Integer, ArrayList<Cluster>>(prev_index, t));
	    }
	    if(next_index != -1) {
		results.add(new Tuple2<Integer, ArrayList<Cluster>>(next_index, t));
	    }
	    return results;
	}
    }

    private static Function<Iterable<ArrayList<Cluster>>, GroupClusters> GROUPCLUSTERS
     = new Function<Iterable<ArrayList<Cluster>>, GroupClusters> () {
	private static final long serialVersionUID = 2785897257033032338L;

	@Override
	public GroupClusters call(Iterable<ArrayList<Cluster>> v1)
		throws Exception {
	    GroupClusters result = new GroupClusters(8);
	    Iterator<ArrayList<Cluster>> itr = v1.iterator();
	    while(itr.hasNext()) {
		result.addCluster(itr.next());
	    }
	    result.validate(); 
	    return result;
	}
	
    };

    public static void main(String[] args) {
	SparkConf conf = new SparkConf();
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
	
	Tuple2<Integer, SnapShot> least_ts = snapshots.max(new Comparator<Tuple2<Integer,SnapShot>>() {
	    @Override
	    public int compare(Tuple2<Integer, SnapShot> o1,
		    Tuple2<Integer, SnapShot> o2) {
		return o1._1 - o2._1;
	    }
	});
	
	Tuple2<Integer, SnapShot> last_ts = snapshots.max(new Comparator<Tuple2<Integer,SnapShot>>() {
	    @Override
	    public int compare(Tuple2<Integer, SnapShot> o1,
		    Tuple2<Integer, SnapShot> o2) {
		return o2._1 - o1._1;
	    }
	});

	// then for each snapshots, we need a DBSCAN
	// afterwards, snapshots contains many ArrayList of clusters. Each
	// ArrayList represent a snapshot.
	JavaRDD<ArrayList<Cluster>> clusters = snapshots.map(DBSCAN);
	
	int L = Integer.parseInt(AppProperties.getProperty("L"));
	int G = Integer.parseInt(AppProperties.getProperty("G"));

	JavaPairRDD<Integer, GroupClusters> 
	groupedCluster = clusters
		.flatMapToPair(new OVERLAPPARTITION(least_ts._1, last_ts._1))
		.groupByKey().mapValues(GROUPCLUSTERS);
	groupedCluster.mapValues(new LOCALCMCMINING(L,G));
	
	
	  
	
//	// Each object has its cluster_id at each valid time-sequence
//	// object with length length < K is filtered first
//	clusters.flatMapToPair(CTO_OCPair)
//		.groupByKey()
//		.filter(TEMPORALFILTER);
//	clusters.repartition();
	
	// then we need to find patterns among those object
	// the object growth algorithm plays a role now
	clusters.collect();
	// clusters.saveAsTextFile(AppProperties.getProperty("hdfs_output"));
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
	    DBSCANClustering dbc = new DBSCANClustering(v1._2);
	    return dbc.cluster();
	}
    };

    // For each cluster, we map it to Oid-> cluster_id->ts pair,
    private static PairFlatMapFunction<ArrayList<Cluster>, Integer, TemporalCluster> CTO_OCPair = new PairFlatMapFunction<ArrayList<Cluster>, Integer, TemporalCluster>() {
	private static final long serialVersionUID = -3031945281249364708L;

	@Override
	public Iterable<Tuple2<Integer, TemporalCluster>> call(
		ArrayList<Cluster> t) throws Exception {
	    ArrayList<Tuple2<Integer, TemporalCluster>> results = new ArrayList<>();
	    for (Cluster c : t) {
		TemporalCluster tc = new TemporalCluster(c.getTS(), c.getID());
		for (Integer oid : c.getObjects()) {
		    results.add(new Tuple2<Integer, TemporalCluster>(oid, tc));
		}
	    }
	    return results;
	}
    };
    
    
    
    // for each list, we examine its temporal pattern, filter those without any matching pattern
    private static Function<Tuple2<Integer, Iterable<TemporalCluster>>, Boolean> TEMPORALFILTER
    = new Function<Tuple2<Integer, Iterable<TemporalCluster>>, Boolean>(){
	/**
	 * 
	 */
	private static final long serialVersionUID = 3632454029397523602L;
	private int K = Integer.parseInt(AppProperties.getProperty("K"));
	private int L = Integer.parseInt(AppProperties.getProperty("L"));
	private int G = Integer.parseInt(AppProperties.getProperty("G"));

	@Override
	public Boolean call(Tuple2<Integer, Iterable<TemporalCluster>> v1)
		throws Exception {
	    boolean valid = true;
	    int num_of_ts = 0;
	    int consecutive = 0;
	    int current = 0;
	    int next = 0;
	    Iterator<TemporalCluster> itr = v1._2.iterator();
	    if(itr.hasNext()) {
		current = itr.next().getTS();
		consecutive = 1;
		num_of_ts ++;
	    }
	    while(itr.hasNext()) {
		next = itr.next().getTS();
		num_of_ts ++;
		if(next == current + 1) {
		   consecutive ++;
		} else {
		    //check gap
		    if(next - current > G) {
			valid = false;
			break;
		    }
		    //check prev length
		    if(consecutive < L) {
			valid = false;
			break;
		    }
		    consecutive = 1; 
		}
		current = next;
	    }
	    if(num_of_ts < K) {
		valid = false;
	    }
	    return valid;
	}
    };
}
