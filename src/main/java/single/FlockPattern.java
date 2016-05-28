package single;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;

import util.DistanceOracle;

import model.Point;
import model.SimpleCluster;
import model.SnapShot;
import model.SnapshotClusters;

public class FlockPattern implements PatternMiner{

    int e,p, m,k;
    ArrayList<SnapShot> input;
    ArrayList<ArrayList<Integer>> patterns;
    public FlockPattern() {
	patterns = new ArrayList<>();
    }
    
    @Override
    public void patternGen() {
	if(input == null) {
	    return;
	} else {
	    long time_start = System.currentTimeMillis();
	    ArrayList<SnapshotClusters> clusters_snapshots = new ArrayList<>();
	    for(SnapShot sp : input) {
		//DBSCANClustering
		DBSCANClustering dbscan = new DBSCANClustering(e,p,sp);
		SnapshotClusters sclusters = new SnapshotClusters(sp.getTS());
		for(SimpleCluster sc : dbscan.cluster()) {
		    sclusters.addCluster(sc);
		}
//		System.out.println(sclusters);
		clusters_snapshots.add(sclusters);
	    }
	    long time_end = System.currentTimeMillis();
	    System.out.print("[Flock]-DBSCAN: " + (time_end-time_start)+ " ms");
	    time_start = System.currentTimeMillis();
	    //then linesweep for finding patterns
	    HashMap<IntSet, Pair<Integer,Integer>> set_start = new HashMap<>();
	    //initialization
	    int t = clusters_snapshots.get(0).getTimeStamp();
	    for(SimpleCluster sc : clusters_snapshots.get(0).getClusters()) {
		if(sc.getObjects().size() >= m) {
		    set_start.put(sc.getObjects(), Pair.of(t, 1));
		}
	    }
	    ArrayList<Pair<ArrayList<Integer>, ArrayList<Integer>>> patterns2 = new ArrayList<>();
	    for(int i = 1; i< clusters_snapshots.size(); i++) {
		SnapshotClusters sclusters = clusters_snapshots.get(i);
		int current = sclusters.getTimeStamp();
		ArrayList<Pair<IntSet, Pair<Integer,Integer>>> tobeadded = new ArrayList<>();
		for(SimpleCluster sc : sclusters.getClusters()) {
		    //check whether any exisiting patterns can be extended
		    IntSet s2 = sc.getObjects();
		    boolean extended = false;
		    for(Entry<IntSet, Pair<Integer, Integer>> e : set_start.entrySet()) {
			IntSet s1 = e.getKey();
			IntSet tmp = new IntOpenHashSet();
			tmp.addAll(s1);
			tmp.retainAll(s2);
			if(tmp.size() == s1.size()) {
			    Pair<Integer, Integer> temporal = Pair.of(e.getValue().getLeft(),
				    				e.getValue().getRight() +1);
			    e.setValue(temporal);
			    extended = true;
			} else {
			    if(tmp.size() >= m) {
				Pair<Integer, Integer> temporal = Pair.of(e.getValue().getLeft(), 
									e.getValue().getRight() + 1);
				tobeadded.add(Pair.of(tmp, temporal));
				extended = true;
				IntSet tmp2 = new IntOpenHashSet();
				tmp2.addAll(s2);
				tmp2.removeAll(s1);
				if(tmp2.size() >= m) {
				    tobeadded.add(Pair.of(tmp2, Pair.of(current, 1)));
				}
			    }    
			}
		    }
		    if(!extended) {
			tobeadded.add(Pair.of(s2, Pair.of(current, 1)));
		    }
		    //scan set_start to remove
		    Iterator<Entry<IntSet, Pair<Integer, Integer>>> itr = set_start.entrySet().iterator();
		    while(itr.hasNext()) {
			Entry<IntSet, Pair<Integer, Integer>> entry = itr.next();
			Pair<Integer,Integer> val = entry.getValue();
			IntSet key = entry.getKey();
			if(val.getLeft() + val.getRight() - 1 != current) {
			    itr.remove();
			    if(val.getRight() >= k) {
				ArrayList<Integer> temporal = new ArrayList<>();
				for(int temp = 0; temp <= val.getRight(); temp++) {
				    temporal.add(val.getKey() + temp);
				}
				patterns2.add(Pair.of(new ArrayList<>(key),temporal));
			    }
			}
		    }
		    //add to be added;
		    for(Pair<IntSet, Pair<Integer,Integer>> tba : tobeadded) {
			set_start.put(tba.getKey(), tba.getValue());
		    }
		}
	    }
	    System.out.println("\tPattern Candidates: " + patterns2.size() + "\t");
	    Iterator<Pair<ArrayList<Integer>, ArrayList<Integer>>> itr = patterns2.iterator();
	    while(itr.hasNext()) {
		Pair<ArrayList<Integer>, ArrayList<Integer>> pattern = itr.next();
		ArrayList<Integer> patter_objects = pattern.getLeft();
		//check whether the pattern is confined in a region withi in e;
		for(int i = 0; i < patter_objects.size(); i++) {
		    for(int j = i+1; j < patter_objects.size(); j++) {
			IntSet candidate = new IntOpenHashSet(patter_objects);
			//candidate are the points that contained in the circle by (i,j) as diameter
			for(int temp : pattern.getRight()) {
			    IntSet current = new IntOpenHashSet();
			    current.add(i);
			    current.add(j);
			    SnapShot ss = input.get(temp);
			    Point pi = ss.getPoint(patter_objects.get(i));
			    Point pj = ss.getPoint(patter_objects.get(j));
			    if(pi == null || pj == null) {
				continue;
			    }
			    Point middle = new Point((pi.getLat() + pj.getLat())/2,
				    		     (pi.getLont() + pj.getLont())/2);
			    for(int k = j+1; k < patter_objects.size(); k++) {
				Point pk = ss.getPoint(patter_objects.get(i));
				if(pk == null) {
				    continue;
				}
				double dist = DistanceOracle.compEarthDistance(
					   pk
					    ,middle);
				if(dist < e) {
				    current.add(k);
				}
			    }
			    candidate.retainAll(current);
			}
			patterns.add(new ArrayList<>(candidate));
		    }
		}
	    }
	    time_end = System.currentTimeMillis();
	    System.out.println("[Flock]-Mining: " + (time_end-time_start) + " ms");
	}
    }

    @Override
    public void loadParameters(int... data) {
	e = data[0];
	p = data[1];
	m = data[2];
	k = data[3];
	System.out.println("[Flock]-Parameters: " + "e="+e+"\tp="+p+"\tm="+m+"\tk="+k);
    }

    @Override
    public void loadData(ArrayList<SnapShot> snapshots) {
	input = new ArrayList<>();
	for(SnapShot ss : snapshots) {
	    input.add(ss.clone());
	}
	input = snapshots;
    }

    @Override
    public void printStats() {

    }
}
