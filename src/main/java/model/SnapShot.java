package model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

/**
 * a Snapshot at time t contains a collection of object
 * with its position value at t
 * @author a0048267
 *
 */
public class SnapShot implements Serializable {
    private static final long serialVersionUID = 4597954049135884174L;
    private int timestamp;
    private HashMap<Integer, Point> object_positions;
    
    public SnapShot(int ts) {
	timestamp = ts;
	object_positions = new HashMap<>();
    }
    
    public void addObject(int oid, Point tp) {
	object_positions.put(oid, tp);
    }
    
    public Set<Integer> getObjects() {
	return object_positions.keySet();
    }
    
    public int getTS() {
	return timestamp;
    }
    
    /**
     * each snapshot has a unique timestamp
     */
    @Override
    public boolean equals(Object snapshot2) {
	if(snapshot2 instanceof SnapShot) {
	    return ((SnapShot) snapshot2).timestamp 
		    == this.timestamp;
	}
	return false;
    }
    
    public void MergeWith(SnapShot sp) {
	if(sp.timestamp != timestamp) {
	    System.err.println("[ERR]: cannot merge Snapshots with different timestamps, expect " 
	+ timestamp + " , merged with " + sp.timestamp);
	} else {
	    object_positions.putAll(sp.object_positions);
	}
    }
    
    @Override
    public String toString() {
	return "<"+timestamp +">:" + object_positions.keySet();
    }

    public Point getPoint(int obj) {
	return object_positions.get(obj);
    }
    
    @Override
    public SnapShot clone() {
	SnapShot nss = new SnapShot(timestamp);
	for(int key : object_positions.keySet()) {
	    nss.addObject(key, object_positions.get(key));
	}
	return nss;
    }
}
