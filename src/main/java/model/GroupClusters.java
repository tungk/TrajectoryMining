package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import common.SerializableComparator;

/**
 * this class implements a group clusters, which combines a set of clusters
 * based on proximate temporal domains on the GroupClusters
 * 
 * @author a0048267
 */
public class GroupClusters implements Serializable,
	Iterable<ArrayList<Cluster>> {
    private static final long serialVersionUID = 8083153530639168232L;
    ArrayList<ArrayList<Cluster>> myClusters;
    private int start;
    private int end; // the starting and ending time sequence of the group
		     // cluster

    // group cluster needs to be consecutive

    public GroupClusters(int size) {
	myClusters = new ArrayList<>(size);
	start = Integer.MAX_VALUE;
	end = -1;
    }

    public int getStart() {
	return start;
    }

    public int getEnd() {
	return end;
    }

    public void addCluster(ArrayList<Cluster> input) {
	int ts = input.get(0).getTS();
	if (ts < start) {
	    start = ts;
	} else if (ts > end) {
	    end = ts;
	}
	myClusters.add(input);
    }

    public ArrayList<Cluster> getClustersAt(int ts) {
	return myClusters.get(ts - start);
    }

    @Override
    public boolean equals(Object obj) {
	if (obj instanceof GroupClusters) {
	    return ((GroupClusters) obj).start == start
		    && ((GroupClusters) obj).end == end;
	} else {
	    return false;
	}
    }

    @Override
    public Iterator<ArrayList<Cluster>> iterator() {
	return myClusters.iterator();
    }

    private void sortByTime() {
	Collections.sort(myClusters,
		new SerializableComparator<ArrayList<Cluster>>() {
		    private static final long serialVersionUID = -1998210671420261478L;
		    @Override
		    public int compare(ArrayList<Cluster> o1,
			    ArrayList<Cluster> o2) {
			int ts1 = o1.get(0).getTS();
			int ts2 = o2.get(0).getTS();
			return ts1 - ts2;
		    }
		}  
	);
    }

    /**
     * make sure the group cluster is valid after calling this method, the
     * clusters are sorted according to the time sequence
     */
    public void validate() {
	sortByTime();
	assert start == myClusters.get(0).get(0).getTS();
	assert end == myClusters.get(myClusters.size() - 1).get(0).getTS();
	assert end - start + 1 == myClusters.size();
    }
}
