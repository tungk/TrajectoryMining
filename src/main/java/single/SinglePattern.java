package single;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.HashSet;

import model.SelfAdjustPattern;
import model.SimpleCluster;
import model.SnapshotClusters;

import org.apache.spark.api.java.JavaRDD;

import util.SetComp;
import util.SetUtils;
import util.SetComp.Result;

import common.SerializableComparator;

public class SinglePattern {
    private int M, L, K, G;
    private JavaRDD<SnapshotClusters> input;

    public SinglePattern(JavaRDD<SnapshotClusters> input, int m, int l, int k,
	    int g) {
	M = m;
	L = l;
	K = k;
	G = g;
	this.input = input;
    }

    public ArrayList<IntSet> runLogic() {
	List<SnapshotClusters> snapshots = input.collect();

	ArrayList<SnapshotClusters> snaps = new ArrayList<>(snapshots);

	Collections.sort(snaps, new SerializableComparator<SnapshotClusters>() {
	    private static final long serialVersionUID = -2377099135785663734L;

	    @Override
	    public int compare(SnapshotClusters o1, SnapshotClusters o2) {
		return o1.getTimeStamp() - o2.getTimeStamp();
	    }
	});

	ArrayList<IntSet> result = new ArrayList<>();
	if (snaps.size() < L) {
	    return result; // not enough inputs
	}
	ArrayList<SelfAdjustPattern> candidates = new ArrayList<>();
	HashSet<IntSet> obj_index = new HashSet<>();
	// insert clusters at snapshot 1 to be the pattern
	SnapshotClusters initial_sp = snaps.get(0);
	int ming_start_time = initial_sp.getTimeStamp();
	for (SimpleCluster cluster : initial_sp.getClusters()) {
	    if (cluster.getSize() >= M) {
		// significant
		SelfAdjustPattern p = new SelfAdjustPattern(M, L, K, G);
		p.addObjects(cluster.getObjects());
		p.growTemporal(initial_sp.getTimeStamp());
		candidates.add(p);
	    }
	}
	int end = snaps.size();
	for (int i = 1; i < end; i++) {
	    SnapshotClusters current_snap = snaps.get(i);
	    int current_ts = current_snap.getTimeStamp();
	    // join current_snapshots with candidate set to see
	    // any chances to form a new pattern
	    for (SimpleCluster cluster : current_snap.getClusters()) {
		// since clusters are disjoint, checking every candidate
		for (int k = 0, can_size = candidates.size(); k < can_size; k++) {
		    SelfAdjustPattern p = candidates.get(k);
		    SetComp comp = SetUtils.compareSets(cluster.getObjects(),
			    p.getObjects());
		    if (comp.getResult() == Result.SUPER
			    || comp.getResult() == Result.EQUAL) {
			// grow the current pattern
			if (!p.growTemporal(current_ts)
				&& !subsetOf(obj_index, p.getObjects())) {
			    SelfAdjustPattern np = new SelfAdjustPattern(M, L,
				    K, G);
			    np.addObjects(p.getObjects());
			    np.growTemporal(current_ts);
			    candidates.add(np);
			}
		    } else if (comp.getResult() == Result.SUB
			    || comp.getResult() == Result.NONE) {
			IntSet commons = comp.getIntersect();
			if (commons.size() >= M
				&& !subsetOf(obj_index, commons)) {
			    // create a new pattern;
			    SelfAdjustPattern newp = new SelfAdjustPattern(M,
				    L, K, G);
			    newp.addObjects(commons);
			    newp.addTemporals(p.getTstamps());
			    if (newp.growTemporal(current_ts)) {
				candidates.add(newp);
			    }
			}
		    }
		}
	    }
	    // further remove unqualified patterns
	    Iterator<SelfAdjustPattern> pitr = candidates.iterator();
	    while (pitr.hasNext()) {
		SelfAdjustPattern p = pitr.next();
		if (p.getEarlistTS() != ming_start_time) {
		    pitr.remove();
		    continue;
		}
		if (current_ts - p.getLatestTS() >= G) { // checking for
							 // expiring, here
		    // we use >=, since in the next
		    // round, every cluster has one
		    // more temporal element, then
		    // >= becomes > automatically
		    pitr.remove();
		    if (p.checkFullValidity()) {
			if (!subsetOf(obj_index, p.getObjects())) {
			    result.add(p.getObjects());
			    addIndex(obj_index, p.getObjects());
			}
		    }
		} else if (p.checkFullValidity()) {
		    if (!subsetOf(obj_index, p.getObjects())) {
			// remove the qualified clusters as soon as possible
			result.add(p.getObjects());
			addIndex(obj_index, p.getObjects());
			pitr.remove();
		    }
		}
	    }
	}
	// dump the patterns in the final cache
	Iterator<SelfAdjustPattern> pitr = candidates.iterator();
	while (pitr.hasNext()) {
	    SelfAdjustPattern p = pitr.next();
	    if (p.checkFullValidity() && !subsetOf(obj_index, p.getObjects())) {
		result.add(p.getObjects());
		addIndex(obj_index, p.getObjects());
	    }
	}
	return result;
    }

    private static boolean subsetOf(Iterable<IntSet> grounds,
	    IntSet test) {
	for (IntSet ground : grounds) {
	    if (ground.containsAll(test)) {
		return true;
	    }
	}
	return false;
    }

    private static void addIndex(HashSet<IntSet> obj_index,
	    IntSet objects) {
	Iterator<IntSet> index_itr = obj_index.iterator();
	boolean contained = false;
	while (index_itr.hasNext()) {
	    IntSet index = index_itr.next();
	    if (objects.containsAll(index)) {
		index_itr.remove();
	    } else if (index.containsAll(objects)) {
		contained = true;
		break;
	    }
	}
	if (!contained) {
	    obj_index.add(objects);
	}
    }
}
