package kforward;

import java.util.ArrayList;
import java.util.Set;
import org.apache.spark.api.java.function.Function;

public class TempoClusters implements Function<ArrayList<SimpleCluster>,ArrayList<SimpleCluster>> {
    private static final long serialVersionUID = 7909428018857050740L;

    @Override
    public ArrayList<SimpleCluster> call(ArrayList<SimpleCluster> v1)
	    throws Exception {
	// TODO Auto-generated method stub
	return null;
    }
//
//    private static final long serialVersionUID = -7608245190871761034L;
//    private int M;
//    
//    public TempoClusters(int M) {
//	this.M = M;
//    }
//    @Override
//    public ArrayList<SimpleCluster> call(ArrayList<Set<Integer>> v1) throws Exception {
//	int time = v1.get(0).getTS();
//	SnapshotCluster base = new SnapshotCluster(time);
//	for(SimpleCluster c : v1) {
//	    if(c.getSize() >= M) {
//		base.addCluster(c);
//	    }
//	}
//	return base;
//    }
}
