package kforward;

import java.util.ArrayList;

import model.Cluster;

import org.apache.spark.api.java.function.Function;

public class TempoClusters implements Function<ArrayList<Cluster>,SnapshotCluster> {

    private static final long serialVersionUID = -7608245190871761034L;
    private int M;
    
    public TempoClusters(int M) {
	this.M = M;
    }
    @Override
    public SnapshotCluster call(ArrayList<Cluster> v1) throws Exception {
	int time = v1.get(0).getTS();
	SnapshotCluster base = new SnapshotCluster(time);
	for(Cluster c : v1) {
	    if(c.getSize() >= M) {
		base.addCluster(c);
	    }
	}
	return base;
    }
}
