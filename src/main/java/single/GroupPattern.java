package single;

import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang3.tuple.Pair;

import util.DistanceOracle;

import model.SnapShot;

public class GroupPattern implements PatternMiner {

    private int l, e, p;
    private ArrayList<SnapShot> input;
    HashMap<Integer, IntSortedSet> obj_temporal;
    ArrayList<ArrayList<Integer>> patterns;
    public GroupPattern() {
	obj_temporal = new HashMap<>();
	patterns = new ArrayList<>();
    }
    
    @Override
    public void patternGen() {
	if (input == null) {
	    return;
	} else {
	    long time_start;
	    long time_end;
	    SwarmPattern sp = new SwarmPattern();
	    sp.loadData2(input);
	    sp.loadParameters2(new int[]{e,p,2,l});
	    sp.patternGen2();
	    time_start = System.currentTimeMillis();
	    ArrayList<Pair<ArrayList<Integer>, ArrayList<Integer>>> patterns2 = sp.getTemporalPatterns();
	    Iterator<Pair<ArrayList<Integer>, ArrayList<Integer>>> itr = patterns2.iterator();
	    while(itr.hasNext()) {
		Pair<ArrayList<Integer>, ArrayList<Integer>> pattern = itr.next();
		//check whether the pattern is confined in a region withi in e;
		boolean inCircle = true;
		for(int i = 0; i < pattern.getLeft().size(); i++) {
		    for(int j = i+1; j < pattern.getLeft().size(); j++) {
			for(int t : pattern.getRight()) {
			    SnapShot ss = input.get(t);
			    double dist =DistanceOracle.compEarthDistance(ss.getPoint(pattern.getLeft().get(i))
				    ,ss.getPoint(pattern.getLeft().get(j)));
			    if(dist > e) {
				inCircle = false;
				break;
			    }
			}
		    }
		}
		if(!inCircle) {
		    itr.remove();
		}
	    }
	    for(Pair<ArrayList<Integer>, ArrayList<Integer>> pp : patterns2) {
		patterns.add(pp.getLeft());
	    }
	    time_end = System.currentTimeMillis();
	    System.out.println("[GROUP]-Mining-2: " + (time_end-time_start) + " ms");
	}
    }

    @Override
    public void loadParameters(int... data) {
	e = data[0];
	p = 5; // for db clustering
	l = data[4];
	System.out.println("[GROUP]-Parameters: " + "e=" + e 
		+ "\tl=" + l);
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
	// TODO Auto-generated method stub
	
    }
}
