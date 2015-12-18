package olpartitioned;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import model.Cluster;
import model.GroupClusters;
import model.Pattern;

import org.apache.spark.api.java.function.Function;

import util.SetCompResult;
import util.SetOps;

/**
 * LocalCMCMiner mines the patterns locally within GroupClusters. A local
 * pattern needs to contain at least M objects, with the temporal constraint be
 * satisfied. Otherwise it is not a pattern
 * 
 * @author a0048267
 * 
 */
public class LocalCMCMiner implements
	Function<GroupClusters, ArrayList<Pattern>> {
    private static final long serialVersionUID = -2307571102823237818L;
    // M is for the object set size
    // K, L and G is for the temporal constraints
    private final int M, L, G, K;

    public LocalCMCMiner(int iM, int iL, int iG, int iK) {
	L = iL;
	G = iG;
	M = iM;
	K = iK;
    }

    @Override
    public ArrayList<Pattern> call(GroupClusters v1) throws Exception {
	ArrayList<Pattern> result = new ArrayList<>();
	int start = v1.getStart();
	int end = v1.getEnd();
	HashSet<Pattern> candidate_sets = new HashSet<>();
	for (int t = start; t <= end; t++) {
	    ArrayList<Cluster> clusters = v1.getClustersAt(t);
	    // see whether any cluster can extend the candidate
	    for (Cluster cluster : clusters) {
		Set<Integer> objects = cluster.getObjects();
		if (t == start) {
		    if (objects.size() >= M) {
			Pattern p = new Pattern();
			p.insertObjects(objects);
			p.insertTime(start);
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
