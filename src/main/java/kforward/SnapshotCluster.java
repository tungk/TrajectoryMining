package kforward;

import java.io.Serializable;
import java.util.ArrayList;

import model.Cluster;

public class SnapshotCluster implements Serializable {
    private static final long serialVersionUID = 4799328581155788723L;
    private ArrayList<Cluster> clusters;
    private int time;
    
    public SnapshotCluster(int time) {
	clusters = new ArrayList<>();
	this.time = time;
    }
    
    public void addCluster(Cluster c) {
	clusters.add(c);
    }
    
    public ArrayList<Cluster> getClusters() {
	return clusters;
    }
    
    public int getTS() {
	return time;
    }
}
