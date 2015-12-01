package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/**
 * this class implements a group clusters, which combines a set of clusters
 * based on proximate temporal domains
 * TODO:: In each reducer, we need to apply the Coherent Moving Cluster(CMC) method
 * on the GroupClusters
 * @author a0048267
 */
public class GroupClusters implements Serializable, Iterable<ArrayList<Cluster>>{
    private static final long serialVersionUID = 8083153530639168232L;
//    private ArrayList<Cluster>[] myClusters;
    ArrayList<ArrayList<Cluster>> myClusters;
    private int start;
    private int end;  // the starting and ending time sequence of the group cluster
//    private int size; // size = end - start +1;
    
//    @SuppressWarnings("unchecked")
//    public GroupClusters(int start, int end) {
//	size = end - start + 1;
//	//we do not need to initialize the actual ArrayList, 
//	//the element of myClusters is a reference to input
////	myClusters = new ArrayList[size];
//    }
    
    public GroupClusters(int size) {
	myClusters = new ArrayList<>(size);
    }
    
    public void addCluster(ArrayList<Cluster> input) {
	int ts = input.get(0).getTS();
	if(ts < start) {
	    start = ts;
	} else if(ts > end) {
	    end = ts;
	}
	myClusters.add(input);
    }
    
    @Override
    public boolean equals(Object obj) {
	if(obj instanceof GroupClusters) {
	    return ((GroupClusters)obj).start == start
		    && ((GroupClusters)obj).end == end;
	} else {
	    return false;
	}
    }

    @Override
    public Iterator<ArrayList<Cluster>> iterator() {
	return myClusters.iterator();
    }

    public void sortByTime() {
	Collections.sort(myClusters, new Comparator<ArrayList<Cluster>>() {
	    @Override
	    public int compare(ArrayList<Cluster> o1, ArrayList<Cluster> o2) {
		int ts1 = o1.get(0).getTS();
		int ts2 = o2.get(0).getTS();
		return ts1-ts2;
	    }});
    }
}
