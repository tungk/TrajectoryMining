package kforward;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import akka.event.slf4j.Logger;
import conf.AppProperties;
import model.Cluster;
import model.SnapShot;

public class KForwordLayout implements Serializable {
    private static final long serialVersionUID = 5312596867491192993L;

    private JavaRDD<String> input;

    private final int K, L, G, M; // these parameters are re-assigned locally

    public KForwordLayout(JavaRDD<String> input) {
	K = Integer.parseInt(AppProperties.getProperty("K"));
	L = Integer.parseInt(AppProperties.getProperty("L"));
	M = Integer.parseInt(AppProperties.getProperty("M"));
	G = Integer.parseInt(AppProperties.getProperty("G"));
	this.input =input;
    }

    public void runLogic() {
	// DBSCAN first
	JavaPairRDD<Integer, SnapShot> TS_CLUSTERS = input
		.filter(new TupleFilter()).mapToPair(new SnapshotGenerator())
		.reduceByKey(new SnapshotCombinor(),
			conf.Constants.SNAPSHOT_PARTITIONS);
	// DBSCAN
	JavaRDD<ArrayList<Cluster>> CLUSTERS = TS_CLUSTERS
		.map(new DBSCANWrapper(conf.Constants.EPS,
			conf.Constants.MINPTS))
		.filter(new Function<ArrayList<Cluster>, Boolean>() {
		    private static final long serialVersionUID = -7032753424628524015L;

		    @Override
		    public Boolean call(ArrayList<Cluster> v1)
			    throws Exception {
			return v1.size() > M;
		    }
		});
	//look at the clusters first
	for(ArrayList<Cluster> clusters : CLUSTERS.collect()) {
	    System.out.println(clusters);
	}
	
    }
}
