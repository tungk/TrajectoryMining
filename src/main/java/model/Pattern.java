package model;

import java.io.Serializable;
import java.util.HashSet;
/**
 * representing trajectory patterns
 * @author a0048267
 *
 */
public class Pattern implements Serializable {
    private static final long serialVersionUID = -2625008459728444860L;
    
    private HashSet<Integer> oids;
    private HashSet<Integer> tss;
    public Pattern() {
	oids = new HashSet<>();
	tss = new HashSet<>();
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
    
    public void insetTime(int ts) {
	tss.add(ts);
    }
    
    public void insertObjectTime(int oid, int ts){
	oids.add(oid);
	tss.add(ts);
    }
    
    @Override
    public String toString() {
	return "[<" + oids +">,<" + tss + ">]";
    }
}
