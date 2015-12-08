package baseline;

import java.util.ArrayList;

import model.Cluster;
import model.SnapShot;

import org.apache.spark.api.java.function.Function;

import cluster.DBSCANClustering;

import scala.Tuple2;

public class DBSCANWrapper implements Function<Tuple2<Integer, SnapShot>, ArrayList<Cluster>> {
    private static final long serialVersionUID = 3562163124094087749L;
    private int eps, minPts;
    public DBSCANWrapper(int ieps, int iminPts){
	eps = ieps;
	minPts = iminPts;
    }
    @Override
    public ArrayList<Cluster> call(Tuple2<Integer, SnapShot> v1)
	    throws Exception {
	    DBSCANClustering dbc = new DBSCANClustering(eps, minPts, v1._2);
	    return dbc.cluster();
    }
}
