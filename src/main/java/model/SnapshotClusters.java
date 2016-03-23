package model;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * The class contains a set of SimpleCluster of at the 
 * same time sequence.
 * 
 * @author a0048267
 *
 */
public class SnapshotClusters implements Serializable {
    private static final long serialVersionUID = 9162568949845610013L;
    private int ts;
    private ArrayList<SimpleCluster> clusters;
    
    public SnapshotClusters(int time){
	ts = time;
	clusters = new ArrayList<>();
    }
    
    public ArrayList<SimpleCluster> getClusters() {
	return clusters;
    }
    
    public SimpleCluster getClusterAt(int index) {
	return clusters.get(index);
    }
    
    public int getTimeStamp() {
	return ts;
    }
    
    public int getClusterSize() {
	return clusters.size();
    }
    
    public void addCluster(SimpleCluster sc) {
	clusters.add(sc);
    }
    
    @Override
    public String toString() {
	return "<"+ ts+ ":"+ clusters+ ">";
    }
}
