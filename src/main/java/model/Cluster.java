package model;

import java.io.Serializable;
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
    private int ID; // this ID is optionally set
    public Cluster(SnapShot sp ) {
	oids = new HashSet<Integer>();
	context = sp;
	ts = sp.getTS();
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
    
    public int getTS() {
	return ts;
    }
    
    public void setID(int id) {
	ID = id;
    }
    
    public int getID() {
	return ID;
    }
    
    @Override
    public String toString() {
	return  ts + "\t" + oids.toString();
    }
}
