package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * representing trajectory patterns
 * 
 * @author a0048267
 * 
 */
public class Pattern implements Serializable {
    private static final long serialVersionUID = -2625008459728444860L;
    private HashSet<Integer> oids;
    private TreeSet<Integer> tss; // tss is thus ordered

    /**
     * create a new placeholder for pattern which consists of two sets OIDs for
     * objects and TSS for time sequence
     */
    public Pattern() {
	oids = new HashSet<>();
	tss = new TreeSet<>();
    }

    public Pattern(int[] os, int[] ts) {
	oids = new HashSet<>();
	tss = new TreeSet<>();
	for(int i : os) {
	    oids.add(i);
	}
	for(int t: ts) {
	    tss.add(t);
	}
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
	tss.add(ts);
    }

    @Override
    public String toString() {
	return "[<" + oids + ">,<" + tss + ">]";
    }

    /**
     * insert a set of objects, attach a timestamp
     * @param objects
     */
    public void insertObjects(Set<Integer> objects) {
	oids.addAll(objects);
    }

    /**
     * insert object set and time set into the pattern
     * 
     * @param objects
     * @param times
     */
    public void insertPattern(Set<Integer> objects, Iterable<Integer> times) {
	oids.addAll(objects);
	for (Integer t_sequence : times) {
	    tss.add(t_sequence);
	}
    }

    public Set<Integer> getObjectSet() {
	return oids;
    }

    /**
     * using native method to get time set, and translate it into an ordered
     * ArrayList
     * 
     * @return
     */
    public List<Integer> getTimeSet() {
	return new ArrayList<Integer>(tss);
    }

    /**
     * get the latest time stamp of this pattern
     * @return
     */
    public int getLatestTS() {
	return tss.last();
    }

    @Override
    public boolean equals(Object p) {
	if (p instanceof Pattern) {
	    Pattern pp = (Pattern) p;
	    return pp.oids.equals(oids);
	}
	return false;
    }

    @Override
    public int hashCode() {
	return oids.hashCode();
    }

    public int getEarlyTS() {
	return tss.first();
    }

}
