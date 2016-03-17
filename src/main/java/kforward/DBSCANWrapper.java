package kforward;

import java.util.ArrayList;

import model.Cluster;
import model.SnapShot;

import org.apache.spark.api.java.function.Function;

import cluster.DBSCANClustering;

import scala.Tuple2;

public class DBSCANWrapper implements Function<Tuple2<Integer, SnapShot>,  ArrayList<SimpleCluster>> {
    private static final long serialVersionUID = 3562163124094087749L;
    private int eps, minPts;
    private int M;
    public DBSCANWrapper(int ieps, int iminPts, int m){
	eps = ieps;
	minPts = iminPts;
	M = m;
    }
    @Override
    public  ArrayList<SimpleCluster> call(Tuple2<Integer, SnapShot> v1)
	    throws Exception {
	    DBSCANClustering dbc = new DBSCANClustering(eps, minPts, v1._2);
	    ArrayList<Cluster> clusters = dbc.cluster();
	    //TODO:: optimize in the future
	    ArrayList<SimpleCluster  > result = new ArrayList<>(); 
	    for(Cluster cluster : clusters) {
		if(cluster.getObjects().size() >= M) {
		    SimpleCluster sc = new SimpleCluster(cluster.getTS());
			sc.addObjects(cluster.getObjects());
			sc.setID(cluster.getID());
			result.add(sc);
		}
	    }
	    return result;
    }
}
