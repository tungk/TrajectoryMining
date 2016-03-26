package kforward;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import model.SimpleCluster;
import model.SnapshotClusters;

import util.SetComp;
import util.SetUtils;
import util.SetComp.Result;

/**
 * This local miner uses line-sweep method to generate patterns. The snapshot
 * clusters are scanned from left to right, meanwhile, every partial pattern is
 * preserved. A pattern is removed either 1) the size of object set is less than
 * M or 2) the gap and l of a cluster is not preserved. In this case, we cannot
 * use the self-adjust pattern object, since we allow the last-tail of the
 * pattern to be invalid.
 * 
 * @author a0048267
 * 
 */

public class LocalMiner implements Serializable {
    private static final long serialVersionUID = 416636556315652980L;
    private int K, M, L, G;

    public LocalMiner(int k, int m, int l, int g) {
	super();
	K = k;
	M = m;
	L = l;
	G = g;
    }

    private ArrayList<SnapshotClusters> input;

    public void setInput(ArrayList<SnapshotClusters> input) {
	this.input = input;
    }

    public ArrayList<HashSet<Integer>> mining() {
	ArrayList<HashSet<Integer>> result = new ArrayList<>();
	if (input.size() < L) {
	    return result; // not enough inputs
	}
	ArrayList<Pattern> candidates = new ArrayList<>();
	HashSet<HashSet<Integer>> obj_index = new HashSet<>();// this index is
							      // used to track
							      // whether a
							      // pattern has
							      // been output or
							      // not
	// insert clusters at snapshot 1 to be the pattern
	SnapshotClusters initial_snapshot = input.get(0);
	for (SimpleCluster cluster : initial_snapshot.getClusters()) {
	    if (cluster.getSize() >= M) {
		// significant
		Pattern p = new Pattern(M, L, K, G);
		p.addObjects(cluster.getObjects());
		p.growTemporal(initial_snapshot.getTimeStamp());
		candidates.add(p);
	    }
	}

	int end = input.size();
	for (int i = 1; i < end; i++) {
	    SnapshotClusters current_snap = input.get(i);
	    int current_ts = current_snap.getTimeStamp();
	    // join current_snapshots with candidate set to see
	    // any chances to form a new pattern
	    for (SimpleCluster cluster : current_snap.getClusters()) {
		// since clusters are disjoint, checking every candidate
		boolean create_single_pattern = true;
		for (int k = 0, can_size = candidates.size(); k < can_size; k++) {
		    Pattern p = candidates.get(k);
		    SetComp comp = SetUtils.compareSets(cluster.getObjects(),
			    p.getObjects());
		    if (comp.getResult() == Result.SUPER
			    || comp.getResult() == Result.EQUAL) {
			// grow the current pattern
			if (!p.growTemporal(current_ts)
				&& !subsetOf(obj_index, p.getObjects())) {
			    Pattern np = new Pattern(M, L, K, G);
			    np.addObjects(p.getObjects());
			    np.growTemporal(current_ts);
			    candidates.add(np);
			}
			create_single_pattern = (comp.getResult() != Result.EQUAL);
		    } else if (comp.getResult() == Result.SUB
			    || comp.getResult() == Result.NONE) {
			Set<Integer> commons = comp.getIntersect();
			if (commons.size() >= M
				&& !subsetOf(obj_index, commons)) {
			    // create a new pattern;
			    Pattern newp = new Pattern(M, L, K, G);
			    newp.addObjects(commons);
			    newp.addTemporals(p.getTstamps());
			    if (!newp.growTemporal(current_ts)) {
				newp.clearTemporals();
				newp.growTemporal(current_ts);
			    }
			    candidates.add(newp);
			}
		    }
		}
		if (create_single_pattern) {
		    if (!subsetOf(obj_index, cluster.getObjects())) {
			Pattern newp = new Pattern(M, L, K, G);
			newp.addObjects(cluster.getObjects());
			newp.growTemporal(current_ts); // a single pattern
						       // always succeed
			candidates.add(newp);
		    }
		}
	    }
	    // further remove unqualified patterns
	    Iterator<Pattern> pitr = candidates.iterator();
	    while (pitr.hasNext()) {
		Pattern p = pitr.next();
		if (current_ts - p.getLatestTS() >= G
			|| current_ts - p.getEarlistTS() >= 2 * K + G) { // checking
									 // for
									 // expiring,
									 // here
									 // we
									 // use
									 // >=,
									 // since
									 // in
									 // the
									 // next
									 // round,
									 // every
									 // cluster
									 // has
									 // one
									 // more
									 // temporal
									 // element,
									 // then
									 // >=
									 // becomes
									 // >
									 // automatically
		    pitr.remove();
		    if (p.checkFullValidity()) {
			if (!subsetOf(obj_index, p.getObjects())) {
			    result.add(p.getObjects());
			    addIndex(obj_index, p.getObjects());
			    // obj_index.add(p.getObjects());
			}
		    } else {
			// remove the pattern only if it is expired && it is not
			// valid
			// obj_index.remove(p.getObjects());
		    }
		}
		if (p.checkFullValidity()) {
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
	Iterator<Pattern> pitr = candidates.iterator();
	while (pitr.hasNext()) {
	    Pattern p = pitr.next();
	    if (p.checkFullValidity() && !subsetOf(obj_index, p.getObjects())) {
		result.add(p.getObjects());
		addIndex(obj_index, p.getObjects());
	    }
	}
	return result;
    }

    public static void main(String[] args) {
	int K = 5;
	int L = 1;
	int G = 5;
	int M = 3;
	ArrayList<SnapshotClusters> input = new ArrayList<>();
	SnapshotClusters sp1 = new SnapshotClusters(1);
	SnapshotClusters sp2 = new SnapshotClusters(2);
	SnapshotClusters sp3 = new SnapshotClusters(3);
	SnapshotClusters sp4 = new SnapshotClusters(4);
	SnapshotClusters sp5 = new SnapshotClusters(5);
	SnapshotClusters sp6 = new SnapshotClusters(6);
	SnapshotClusters sp7 = new SnapshotClusters(7);
	SnapshotClusters sp8 = new SnapshotClusters(8);
	SnapshotClusters sp9 = new SnapshotClusters(9);
	SnapshotClusters sp10 = new SnapshotClusters(10);
	sp1.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3)));
	sp1.addCluster(new SimpleCluster(Arrays.asList(4, 5, 6)));
	sp1.addCluster(new SimpleCluster(Arrays.asList(7, 8, 9)));

	sp2.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3, 4)));
	sp2.addCluster(new SimpleCluster(Arrays.asList(5, 6, 7, 8, 9)));

	sp3.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8,
		9)));

	sp4.addCluster(new SimpleCluster(Arrays.asList(1, 2, 4)));
	sp4.addCluster(new SimpleCluster(Arrays.asList(3, 5, 6)));
	sp4.addCluster(new SimpleCluster(Arrays.asList(7, 8, 9)));

	sp5.addCluster(new SimpleCluster(Arrays.asList(1, 2, 7, 8, 9)));
	sp5.addCluster(new SimpleCluster(Arrays.asList(4, 5, 6)));
	sp5.addCluster(new SimpleCluster(Arrays.asList(3)));

	sp6.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3, 4)));
	sp6.addCluster(new SimpleCluster(Arrays.asList(5, 6, 7)));
	sp6.addCluster(new SimpleCluster(Arrays.asList(8, 9)));

	sp7.addCluster(new SimpleCluster(Arrays.asList(1, 8, 9)));
	sp7.addCluster(new SimpleCluster(Arrays.asList(2, 3, 4)));
	sp7.addCluster(new SimpleCluster(Arrays.asList(5, 6, 7)));

	sp8.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3, 4)));
	sp8.addCluster(new SimpleCluster(Arrays.asList(5, 6)));
	sp8.addCluster(new SimpleCluster(Arrays.asList(7, 8, 9)));

	sp9.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3)));
	sp9.addCluster(new SimpleCluster(Arrays.asList(4, 5, 6)));
	sp9.addCluster(new SimpleCluster(Arrays.asList(7, 8, 9)));

	sp10.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3)));
	sp10.addCluster(new SimpleCluster(Arrays.asList(4, 5, 8)));
	sp10.addCluster(new SimpleCluster(Arrays.asList(6, 7, 9)));

	input.add(sp1);
	input.add(sp2);
	input.add(sp3);
	input.add(sp4);
	input.add(sp5);
	input.add(sp6);
	input.add(sp7);
	input.add(sp8);
	input.add(sp9);
	input.add(sp10);

	for (SnapshotClusters in : input) {
	    System.out.println(in);
	}
	LocalMiner lm = new LocalMiner(K, M, L, G);

	lm.setInput(input);
	ArrayList<HashSet<Integer>> result = lm.mining();

	int index = 0;
	for (HashSet<Integer> cluster : result) {
	    System.out.println((index++) + "\t" + cluster);
	}
    }

    /**
     * clear the current status of the lm
     */
    public void clear() {
    }

    private static boolean subsetOf(Collection<HashSet<Integer>> grounds,
	    Set<Integer> test) {
	for (HashSet<Integer> ground : grounds) {
	    if (ground.containsAll(test)) {
		return true;
	    }
	}
	return false;
    }

    private static void addIndex(HashSet<HashSet<Integer>> obj_index,
	    HashSet<Integer> objects) {
	Iterator<HashSet<Integer>> index_itr = obj_index.iterator();
	boolean contained = false;
	while (index_itr.hasNext()) {
	    HashSet<Integer> index = index_itr.next();
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
