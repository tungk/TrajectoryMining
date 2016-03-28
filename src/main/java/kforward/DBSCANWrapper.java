package kforward;

import java.util.ArrayList;

import model.SimpleCluster;
import model.SnapShot;
import model.SnapshotClusters;

import org.apache.spark.api.java.function.Function;


import scala.Tuple2;
import util.DBSCANClustering;

public class DBSCANWrapper implements Function<Tuple2<Integer, SnapShot>,  SnapshotClusters> {
    private static final long serialVersionUID = 3562163124094087749L;
    private int eps, minPts;
    private int M;
    public DBSCANWrapper(int ieps, int iminPts, int m){
	eps = ieps;
	minPts = iminPts;
	M = m;
    }
    @Override
    public SnapshotClusters call(Tuple2<Integer, SnapShot> v1)
	    throws Exception {
	    DBSCANClustering dbc = new DBSCANClustering(eps, minPts, v1._2);
	    ArrayList<SimpleCluster> clusters = dbc.cluster();
	    SnapshotClusters result = new SnapshotClusters(v1._1); 
	    for(SimpleCluster cluster : clusters) {
		if(cluster.getObjects().size() >= M) {
		    SimpleCluster sc = new SimpleCluster();
			sc.addObjects(cluster.getObjects());
			sc.setID(cluster.getID());
			result.addCluster(sc);
		}
	    }
	    return result;
    }
}
