package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * the cluster contains a set of object id together with
 * a reference to its context (SnapShot), where its location information is
 * stored
 * @author a0048267
 *
 */
public class Cluster implements Serializable{
    private static final long serialVersionUID = -3234023276731997723L;
    private int ts;
    private final SnapShot context;
    private HashSet<Integer> oids;
    private String ID; // this ID is optionally set
    public Cluster(SnapShot sp ) {
	oids = new HashSet<Integer>();
	context = sp;
	ts = sp.getTS();
    }
    
    /**
     * This constructor is used for testing purpose, where
     * we can create a cluster at a specific time sequence;
     * @param ts
     */
    public Cluster(int ts, String id) {
	context = new SnapShot(ts);
	this.ts = ts;
	ID = id;
    }
    
    public int getSize() {
	return oids.size();
    }
    
    public Set<Integer> getObjects () {
	return oids;
    }
    
    public void addObject(int obj) {
	assert context.getObjects().contains(obj);
	oids.add(obj);
    }
    
    public Point getPosition(int oid) {
	if(oids.contains(oid)) {
	    return context.getPoint(oid);
	} else {
	    return null;
	}
    }
    
    public SnapShot getContext() {
	return context;
    }
    
    public int getTS() {
	return ts;
    }
    
    public void setID(String id) {
	ID = id;
    }
    
    public String getID() {
	return ID;
    }
    
    @Override
    public String toString() {
	return  "<"+ID+":"+ ts + "\t" + oids.toString() +">";
    }

    public void addAllObjects(ArrayList<Integer> combination) {
	oids.addAll(combination);
    }
}
