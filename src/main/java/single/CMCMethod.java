package single;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import model.Cluster;
import model.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import util.SetCompResult;
import util.SetOps;

import baseline.DBSCANWrapper;
import baseline.SnapshotCombinator;
import baseline.SnapshotGenerator;
import baseline.TupleFilter;

import conf.AppProperties;

public class CMCMethod {
    private final int K, L, M, G, eps, minPts;
    private final String hdfs_input;
    private final JavaSparkContext jsc;

    public CMCMethod() {
	K = Integer.parseInt(AppProperties.getProperty("K"));
	L = Integer.parseInt(AppProperties.getProperty("L"));
	M = Integer.parseInt(AppProperties.getProperty("M"));
	G = Integer.parseInt(AppProperties.getProperty("G"));
	eps = Integer.parseInt(AppProperties.getProperty("eps"));
	minPts = Integer.parseInt(AppProperties.getProperty("minPts"));
	hdfs_input = AppProperties.getProperty("hdfs_input");
	SparkConf conf = new SparkConf();
	if (!conf.contains("spark.app.name")) {
	    conf = conf.setAppName(AppProperties.getProperty("appName"));
	}
	if (!conf.contains("spark.master")) {
	    conf = conf.setMaster(AppProperties.getProperty("spark_master"));
	}
	jsc = new JavaSparkContext(conf);
    }

    /**
     * this ensures the CMC method uses the same trajectory data as parallel
     * solutions
     * 
     * @return
     */
    private JavaRDD<String> readTrajectoryFromHDFS() {
	return jsc.textFile(hdfs_input);
    }

    public ArrayList<Pattern> runLogic() {
	JavaRDD<String> input = readTrajectoryFromHDFS();
	// use spark for data preprocessing
	List<ArrayList<Cluster>> snapshots = input.filter(new TupleFilter())
		.mapToPair(new SnapshotGenerator())
		.reduceByKey(new SnapshotCombinator())
		.map(new DBSCANWrapper(eps, minPts)).collect();
	System.out.println("Input Read from Spark");
	jsc.close();
	System.out.println("Spark Closed");
	// from this point onwards, the code is run on the driver nodes
	long time_now = System.currentTimeMillis();
	Collections.sort(snapshots, new Comparator<ArrayList<Cluster>>() {
	    @Override
	    public int compare(ArrayList<Cluster> o1, ArrayList<Cluster> o2) {
		return o1.get(0).getTS() - o2.get(0).getTS();
	    }
	});
	ArrayList<Pattern> patterns = CMC(snapshots);
	System.out.println("Patterns Generated in: "
		+ (System.currentTimeMillis() - time_now) + " ms" + "\t" + patterns.size());
	int index = 0;
	for(Pattern p : patterns) {
	    System.out.println(index  + "\t" + p);
	    index++;
	}
	return patterns;
    }

    /**
     * the CMC algorithm used the snapshots are already sorting in time sequence
     * 
     * @param snapshots
     * @return
     */
    private ArrayList<Pattern> CMC(List<ArrayList<Cluster>> snapshots) {
	ArrayList<Pattern> result = new ArrayList<>();
	int size = snapshots.size();
	HashSet<Pattern> candidate_sets = new HashSet<>();
	for (int t = 0; t < size; t++) {
	    ArrayList<Cluster> clusters = snapshots.get(t);
	    // see whether any cluster can extend the candidate
	    for (Cluster cluster : clusters) {
		Set<Integer> objects = cluster.getObjects();
		if (t == 0) {
		    if (objects.size() >= M) {
			Pattern p = new Pattern();
			p.insertObjects(objects);
			p.insertTime(0);
			candidate_sets.add(p);
		    }
		} else {
		    // intersect with existing patterns
		    Set<Pattern> tobeadded = new HashSet<Pattern>();
		    boolean singleton = true; // singleton checks if the current
					      // cluster does not extend any
					      // pattern
		    for (Pattern p : candidate_sets) {
			if (p.getLatestTS() < t) {
			    // here we discuss three cases,
			    // p \subseteq c,
			    // c \subseteq p
			    // p \cap c \neq \emptyset
			    Set<Integer> pattern_objects = p.getObjectSet();
			    SetCompResult scr = SetOps.setCompare(
				    pattern_objects, objects);
			    if (scr.getStatus() == 0) {
				// no containments occur
				if (scr.getCommonsSize() >= M) {
				    // make a new pattern;
				    Pattern newp = new Pattern();
				    newp.insertPattern(scr.getCommons(),
					    p.getTimeSet());
				    newp.insertTime(t);
				    tobeadded.add(newp);
				}
			    } else if (scr.getStatus() == 1) {
				// pattern contain objects
				// this coincide with status 0
				singleton = false;
			    } else if (scr.getStatus() == 2) {
				// object contains pattern
				// object itself needs to be a pattern
				Pattern newp2 = new Pattern();
				// newp2.insertPattern(objects,
				// new ArrayList<Integer>(), t);
				newp2.insertPattern(objects,
					new ArrayList<Integer>());
				// extend newp2;
				newp2.insertTime(t);
				tobeadded.add(newp2);
				p.insertTime(t);
				singleton = false;
			    } else if (scr.getStatus() == 3) {
				// object equals pattern
				// extends p by one more timestamp
				p.insertTime(t);
				singleton = false;
			    }
			}
		    }
		    if (singleton) {
			// create a pattern for objects at time t
			Pattern newp2 = new Pattern();
			newp2.insertPattern(objects, new ArrayList<Integer>());
			newp2.insertTime(t);
			tobeadded.add(newp2);
		    }
		    candidate_sets.addAll(tobeadded);
		}
	    }
	    // before moving to next sequence, filter all necessary patterns
	    HashSet<Pattern> toberemoved = new HashSet<>();
	    for (Pattern p : candidate_sets) {
		if (t != p.getLatestTS()) {
		    // check l-consecutiveness
		    List<Integer> sequences = p.getTimeSet();
		    int cur_consecutive = 1;
		    for (int ps = 1, len = sequences.size(); ps < len; ps++) {
			if (sequences.get(ps) - sequences.get(ps - 1) == 1) {
			    cur_consecutive++;
			} else {
			    if (cur_consecutive < L) {
				toberemoved.add(p);
				break;
			    } else {
				cur_consecutive = 0;
			    }
			}
		    }
		}
		if (t - p.getLatestTS() > G) {
		    toberemoved.add(p);
		}
		// this should not happen
		if (p.getObjectSet().size() < M) {
		    toberemoved.add(p);
		}
	    }
	    candidate_sets.removeAll(toberemoved);
	}
	for (Pattern p : candidate_sets) {
	    if (p.getTimeSet().size() >= K) {
		result.add(p);
	    }
	}
	return result;
    }
}
