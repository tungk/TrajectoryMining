package twophasejoin;

import java.util.ArrayList;

import model.Cluster;
import model.SnapShot;

import org.apache.spark.api.java.function.Function;

import cluster.DBSCANClustering;

public class DBSCANWrapper implements Function<SnapShot, ArrayList<Cluster>> {
    private static final long serialVersionUID = 3562163124094087749L;
    private int eps, minPts;

    public DBSCANWrapper(int ieps, int iminPts) {
	eps = ieps;
	minPts = iminPts;
    }

    @Override
    public ArrayList<Cluster> call(SnapShot v1) throws Exception {
	DBSCANClustering dbc = new DBSCANClustering(eps, minPts, v1);
	return dbc.cluster();
    }
}
