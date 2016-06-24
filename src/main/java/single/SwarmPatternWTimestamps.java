package single;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.util.ArrayList;
import java.util.HashMap;

import model.SimpleCluster;
import model.SnapShot;
import model.SnapshotClusters;

import org.apache.commons.lang3.tuple.Pair;

public class SwarmPatternWTimestamps {
    private int e, p, m, k;
    ArrayList<SnapShot> input;
    HashMap<Integer, IntSortedSet> obj_temporal;
    ArrayList<Pair<ArrayList<Integer>, ArrayList<Integer>>> patterns;
    private int r;

    public SwarmPatternWTimestamps() {
	obj_temporal = new HashMap<>();
	patterns = new ArrayList<>();
    }
    
    public  ArrayList<Pair<ArrayList<Integer>, ArrayList<Integer>>> getTemporalPatterns() {
	return patterns;
    }
    
    public void patternGen() {
	if (input == null) {
	    return;
	} else {
	    long time_start, time_end;
	    time_start = System.currentTimeMillis();
	    ArrayList<SnapshotClusters> clusters_snapshots = new ArrayList<>();
	    for (SnapShot sp : input) {
		// DBSCANClustering
		DBSCANClustering dbscan = new DBSCANClustering(e, p, sp,r);
		int time = sp.getTS();
		SnapshotClusters sclusters = new SnapshotClusters(time);
		for (SimpleCluster sc : dbscan.cluster()) {
		    sclusters.addCluster(sc);
		    // int cluster_id = Integer.parseInt(sc.getID());
		    for (Integer object : sc.getObjects()) {
			if (!obj_temporal.containsKey(object)) {
			    obj_temporal.put(object, new IntRBTreeSet());
			}
			obj_temporal.get(object).add(time);
		    }
		}
		// System.out.println(sclusters);
		clusters_snapshots.add(sclusters);
	    }
	    time_end = System.currentTimeMillis();
	    System.out.println("[GROUP]-DBSCAN: " + (time_end - time_start) + " ms");
	    // Object search starts from obj_temporal map
	    time_start = System.currentTimeMillis();
	    mineSwarm();
	    time_end = System.currentTimeMillis();
	    System.out.println("[GROUP]-MINING-1: " + (time_end - time_start) + " ms");
	}
    }
    

    private void mineSwarm() {
	ArrayList<Integer> Omax = new ArrayList<>(obj_temporal.keySet());
	IntSortedSet Tmax = new IntRBTreeSet();
	for (IntSortedSet value : obj_temporal.values()) {
	    Tmax.addAll(value);
	}
	ObjectGrowth(new ArrayList<Integer>(), Tmax, -1, Omax, Tmax.size());
    }
    
    private void ObjectGrowth(ArrayList<Integer> Oset, IntSortedSet Tmax, int olast_index,
	    ArrayList<Integer> Omax, int tsize) {
	if (Tmax.size() < k) {
	    return;
	}
	if (BackwardPruning(olast_index, Oset, Tmax, Omax)) {
	    boolean forward_closure = true;
	    for (int i = olast_index + 1; i < Omax.size(); i++) {
		int o = Omax.get(i);
		ArrayList<Integer> Osetprime = new ArrayList<Integer>(Oset);
		Osetprime.add(o);
		IntSortedSet Tprime = GenerateMaxTimeSet(o, olast_index, Tmax, Omax);
		if (Tprime.size() == Tmax.size()) {
		    forward_closure = false;
		}
		ObjectGrowth(Osetprime, Tprime, i, Omax, tsize);
	    }
	    if (forward_closure && Oset.size() >= m) {
		patterns.add(Pair.of(new ArrayList<Integer>(Oset), new ArrayList<Integer>(Tmax)));
	    }
	}
    }

    private IntSortedSet GenerateMaxTimeSet(int i, int olast, IntSortedSet tmax, ArrayList<Integer> omax) {
	IntSortedSet Tprime = new IntRBTreeSet();
	if (olast == -1) {
	    Tprime.addAll(obj_temporal.get(i));
	} else {
	    IntSortedSet o1 = obj_temporal.get(i);
	    IntSortedSet o2 = obj_temporal.get(omax.get(olast));

	    for (int t : tmax) {
		if (o1.contains(t) && o2.contains(t)) {
		    Tprime.add(t);
		}
	    }
	}
	return Tprime;
    }

    private boolean BackwardPruning(int olast, ArrayList<Integer> oset,
	    IntSortedSet tmax, ArrayList<Integer> omax) {
	if(olast == -1) {
	    return true;
	}
	int oj = omax.get(olast);
	for(int i = 0; i < omax.size(); i++) {
	    int o = omax.get(i);
	    if (!oset.contains(o)) {
		if (i < olast) {
		    IntSortedSet t1 = obj_temporal.get(o);
		    IntSortedSet t2 = obj_temporal.get(oj);
		    IntSortedSet t3 = new IntRBTreeSet();
		    t3.addAll(t1);
		    t3.retainAll(t2);
		    if (t3.containsAll(tmax)) {
			return false;
		    }
		}
	    }
	}
	return true;
    }

    public void loadParameters(int e, int p, int m, int k, int r) {
	this.e = e;
	this.p = p;
	this.m = m;
	this.k = k;
	this.r = r;
    }
   
    public void loadData(ArrayList<SnapShot> snapshots) {
	input = new ArrayList<>();
	for(SnapShot ss : snapshots) {
	    input.add(ss.clone());
	}
	input = snapshots;
    }


}
