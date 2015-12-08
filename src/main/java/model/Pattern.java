package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
/**
 * representing trajectory patterns
 * @author a0048267
 *
 */
public class Pattern implements Serializable {
    private static final long serialVersionUID = -2625008459728444860L;
    
    private HashSet<Integer> oids;
    private ArrayList<Integer> tss; // tss is ordered

    private int latest_ts;
    public Pattern() {
	oids = new HashSet<>();
	tss = new ArrayList<>();
	latest_ts = 0;
    }
    
    public int getObjectSize() {
	return oids.size();
    }
    public int getTimeSize() {
	return tss.size();
    }
    
    public void insertObject(int oid) {
	oids.add(oid);
    }
    
    public void insertTime(int ts) {
	if(ts > latest_ts) {
	    latest_ts = ts;
	}
	tss.add(ts);
    }
    
    public void insertObjectTime(int oid, int ts){
	oids.add(oid);
	insertTime(ts);
    }
    
    @Override
    public String toString() {
	return "[<" + oids +">,<" + tss + ">]";
    }

    /**
     * insert an object set, attach a timestamp
     * @param objects
     * @param start
     */
    public void insertObjects(Set<Integer> objects, int start) {
	oids.addAll(objects);
	insertTime(start);
    }
    
    public void insertPattern(Set<Integer> objects, List<Integer> times, int latest) {
	oids.addAll(objects);
	tss.addAll(times);
	tss.add(latest);
	latest_ts = latest;
    }
    
    public Set<Integer> getObjectSet() {
	return oids;
    }
    
    public List<Integer> getTimeSet() {
	return tss;
    }
    
    public int getLatestTS() {
	return latest_ts;
    }
    
    @Override
    public boolean equals(Object p) {
	if(p instanceof Pattern) {
	    Pattern pp = (Pattern) p;
	    return pp.oids.equals(oids);
	}
	return false;
    }
    
    @Override
    public int hashCode() {
	return oids.hashCode();
    }
    
}
