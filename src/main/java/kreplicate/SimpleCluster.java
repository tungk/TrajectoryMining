package kreplicate;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SimpleCluster is similar with {@link Cluster} object,
 * but it removes the reference to {@link SnapShot} object, thus is
 * more serialization friendly
 * @author a0048267
 *
 */
public class SimpleCluster implements Serializable{
    private static final long serialVersionUID = 7061586699944678710L;
    private int ts;
    private HashSet<Integer> oids;
    private String ID; // this ID is optionally set
    public SimpleCluster(int ts) {
	oids = new HashSet<Integer>();
	this.ts = ts;
    }
    
    /**
     * This constructor is used for testing purpose, where
     * we can create a cluster at a specific time sequence;
     * @param ts
     */
    public SimpleCluster(int ts, String id) {
	oids = new HashSet<>();
	this.ts = ts;
	ID = id;
	
    }
    
    public SimpleCluster(int ts, String id, int[] oids) {
	this(ts,id);
	for(int i : oids) {
	    this.oids.add(i);
	}
    }
    
    public SimpleCluster(int i, List<Integer> asList) {
	this.ts = i;
	this.oids = new HashSet<Integer>(asList);
    }

    public int getSize() {
	return oids.size();
    }
    
    public HashSet<Integer> getObjects () {
	return oids;
    }
    
    public void addObject(int obj) {
	oids.add(obj);
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

    public void addObjects(Set<Integer> objects) {
	oids.addAll(objects);
    }

}
