package baseline;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import model.Cluster;
import model.Pattern;
import model.SnapShot;
import olpartitioned.DBSCANWrapper;
import olpartitioned.SnapshotCombinator;
import olpartitioned.SnapshotGenerator;
import olpartitioned.TupleFilter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import util.SetCompResult;
import util.SetOps;
import util.TemporalVerification;


public class Layout implements Serializable {
    private static final long serialVersionUID = 8212869092328285729L;
    private JavaRDD<String> inputs;
    public Layout(JavaRDD<String> textFile) {
	inputs = textFile;
    }

    private JavaPairRDD<Integer, SnapShot> genSnapshots() {
	return inputs.filter(new TupleFilter())
		.mapToPair(new SnapshotGenerator())
		.reduceByKey(new SnapshotCombinator(), conf.Constants.SNAPSHOT_PARTITIONS);
    }

    public void runLogic() {
	// inputs is ready
	System.out.println("Input Ready");
	JavaPairRDD<Integer, SnapShot> snapshots = genSnapshots();
	System.out.println("Snapshots Ready");
	JavaRDD<ArrayList<Cluster>> clusters = snapshots.map(new DBSCANWrapper(
		conf.Constants.EPS, conf.Constants.MINPTS));
	System.out.println("DBSCAN Ready");
	JavaPairRDD<Integer, ArrayList<Pattern>> local_patterns = clusters
		.mapToPair(new PairFunction<ArrayList<Cluster>, Integer, ArrayList<Pattern>>() {
		    private static final long serialVersionUID = 293055163608994485L;

		    @Override
		    public Tuple2<Integer, ArrayList<Pattern>> call(
			    ArrayList<Cluster> t) throws Exception {
			ArrayList<Pattern> p = new ArrayList<Pattern>();
			int ts = t.get(0).getTS();
			for (Cluster c : t) {
			    Pattern pt = new Pattern();
			    pt.insertObjects(c.getObjects());
			    pt.insertTime(ts);
			    p.add(pt);
			}
			return new Tuple2<Integer, ArrayList<Pattern>>(ts, p);
		    }
		});
	System.out.println("Join Starts:");
	long count = local_patterns.count();
	while (count > 1) {
	    System.out.println("\tRound:\t" + count);
	    local_patterns = local_patterns.mapToPair(STAGE_MAP).reduceByKey(
		    STAGE_REDUCE);
	    count = local_patterns.count();
	}
	System.out.println("Filter Starts");
	List<Pattern> final_patterns = local_patterns
		.flatMap(
			new FlatMapFunction<Tuple2<Integer, ArrayList<Pattern>>, Pattern>() {
			    private static final long serialVersionUID = -5043868653091837942L;

			    @Override
			    public Iterable<Pattern> call(
				    Tuple2<Integer, ArrayList<Pattern>> t)
				    throws Exception {
				return t._2;
			    }
			}).filter(new Function<Pattern, Boolean>() {
		    private static final long serialVersionUID = -2043561468644039317L;

		    @Override
		    public Boolean call(Pattern v1) throws Exception {
			if (v1.getObjectSize() < conf.Constants.K) {
			    return false;
			} else {
			    return TemporalVerification.isValidTemporal(
				    v1.getTimeSet(), conf.Constants.K, conf.Constants.L, conf.Constants.G);
			}
		    }
		}).collect();
	System.out.println("Filter Ends");
	System.out.println(final_patterns.size());
	for (Pattern p : final_patterns) {
	    System.out.println(p);
	}
    }

    private static PairFunction<Tuple2<Integer, ArrayList<Pattern>>, Integer, ArrayList<Pattern>> STAGE_MAP = new PairFunction<Tuple2<Integer, ArrayList<Pattern>>, Integer, ArrayList<Pattern>>() {
	private static final long serialVersionUID = 2997493391442263325L;

	@Override
	public Tuple2<Integer, ArrayList<Pattern>> call(
		Tuple2<Integer, ArrayList<Pattern>> t) throws Exception {
	    return new Tuple2<Integer, ArrayList<Pattern>>(t._1 / 2, t._2);
	}
    };

    private static Function2<ArrayList<Pattern>, ArrayList<Pattern>, ArrayList<Pattern>> STAGE_REDUCE = new Function2<ArrayList<Pattern>, ArrayList<Pattern>, ArrayList<Pattern>>() {
	private static final long serialVersionUID = 6875935653511633616L;

	@Override
	public ArrayList<Pattern> call(ArrayList<Pattern> v1,
		ArrayList<Pattern> v2) throws Exception {
	    ArrayList<Pattern> result = new ArrayList<>();
	    HashSet<Pattern> excluded = new HashSet<Pattern>();
	    // TODO:: further pruning exists. For
	    // example, we can prune the
	    // patterns that has is far beyond gap G
	    for (Pattern p1 : v1) {
		for (Pattern p2 : v2) {
		    if (p1.getEarlyTS() - p2.getLatestTS() > conf.Constants.G) {
			continue;
		    }
		    if (p2.getEarlyTS() - p1.getLatestTS() > conf.Constants.G) {
			continue; // no need to consider
		    }
		    Set<Integer> s1 = p1.getObjectSet();
		    Set<Integer> s2 = p2.getObjectSet();
		    SetCompResult scr = SetOps.setCompare(s1, s2);
		    // check common size
		    if (scr.getCommonsSize() < conf.Constants.M) {
			// then the only pattern is p1
			// and p2
			continue;
		    }
		    if (scr.getStatus() == 1) {
			// p1 contains p2, so we exclude
			// p2
			excluded.add(p2);
		    }
		    if (scr.getStatus() == 2) {
			// p2 contains p1, so we exclude
			// p1
			excluded.add(p1);
		    }
		    if (scr.getStatus() == 3) {
			// p1 equals p2, so we exclude
			// both
			excluded.add(p2);
			excluded.add(p1);
		    }
		     // common size greater than M
		    // create a new pattern based on the
		    // common size
		    Pattern newp = new Pattern();
		    newp.insertObjects(scr.getCommons());
		    for (Integer t : p1.getTimeSet()) {
			newp.insertTime(t);
		    }
		    for (Integer t : p2.getTimeSet()) {
			newp.insertTime(t);
		    }
		    result.add(newp);
		}
	    }

	    // prune out-of-ranged patterns
	    int p1_latest = -1, p1_earliest = Integer.MAX_VALUE;
	    int p2_latest = -1, p2_earliest = Integer.MAX_VALUE;
	    for (Pattern p1 : v1) {
		if (p1.getLatestTS() > p1_latest) {
		    p1_latest = p1.getLatestTS();
		}
		if (p1.getEarlyTS() < p1_earliest) {
		    p1_earliest = p1.getEarlyTS();
		}
	    }
	    for (Pattern p2 : v2) {
		if (p2.getLatestTS() > p2_latest) {
		    p2_latest = p2.getLatestTS();
		}
		if (p2.getEarlyTS() < p2_earliest) {
		    p2_earliest = p2.getEarlyTS();
		}
	    }
	    int earliest = p1_earliest > p2_earliest ? p2_earliest
		    : p1_earliest;
	    int latest = p1_latest > p2_latest ? p1_latest : p2_latest;
	    for (Pattern p1 : v1) {
		if (p1.getEarlyTS() - earliest > conf.Constants.G
			&& latest - p1.getLatestTS() > conf.Constants.G) {
		    excluded.add(p1);
		}
		if (p1.getEarlyTS() - earliest != 0
			|| latest - p1.getLatestTS() != 0) {
		    if (p1.getTimeSize() < conf.Constants.L) {
			excluded.add(p1);
		    }
		}
	    }
	    for (Pattern p2 : v2) {
		if (p2.getEarlyTS() - earliest > conf.Constants.G
			&& latest - p2.getLatestTS() > conf.Constants.G) {
		    excluded.add(p2);
		}
		if (p2.getEarlyTS() - earliest != 0
			|| latest - p2.getLatestTS() != 0) {
		    if (p2.getTimeSize() < conf.Constants.L) {
			excluded.add(p2);
		    }
		}
	    }
	    // for each old patterns, determine whether
	    // to further include p1 or
	    // p2
	    // this can prune many unnecessary patterns
	    for (Pattern p1 : v1) {
		if (p1.getObjectSize() >= conf.Constants.M) {
		    if (!excluded.contains(p1)) {
			result.add(p1);
		    }
		}
	    }
	    for (Pattern p2 : v2) {
		if (p2.getObjectSize() >= conf.Constants.M) {
		    if (!excluded.contains(p2)) {
			result.add(p2);
		    }
		}
	    }
	    return result;
	}
    };
}
