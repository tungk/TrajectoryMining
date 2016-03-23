package model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * A SimpleCluster is more serialization friendly. It only 
 * serialize the objects set rather than referencing objects.
 * 
 * A cluster needs to have a time ID, though it may be redundant
 * as multiple clusters belonging to the same snapshots containing the same ID.
 * 
 * TODO:: May use BitSet or FastUtil set to further boost the performance
 * @author a0048267
 *
 */
public class SimpleCluster implements Serializable{
    private static final long serialVersionUID = 7061586699944678710L;
    private HashSet<Integer> oids;
    private String ID; // this ID is optionally set
  
    public SimpleCluster() {
	oids = new HashSet<Integer>();
    }
    
    /**
     * This constructor is used for testing purpose, where
     * we can create a cluster at a specific time sequence;
     * @param ts
     */
    public SimpleCluster(Iterable<Integer> list) {
	this();
	for(int i : list) {
	    this.oids.add(i);
	}
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
    
    public void setID(String id) {
	ID = id;
    }
    
    public String getID() {
	return ID;
    }
    
    @Override
    public String toString() {
	return  "<"+ID+":"+ oids.toString() +">";
    }

    public void addObjects(Set<Integer> objects) {
	oids.addAll(objects);
    }

}
