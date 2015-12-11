package baseline;

import java.util.ArrayList;
import java.util.Iterator;

import model.Cluster;
import model.GroupClusters;

import org.apache.spark.api.java.function.Function;

public class ClusterGrouper implements
	Function<Iterable<ArrayList<Cluster>>, GroupClusters> {
    private static final long serialVersionUID = 5022639824369533556L;
    private int initial_size;
    public ClusterGrouper(int isize) {
	initial_size = isize;
    }
    @Override  
    public GroupClusters call(Iterable<ArrayList<Cluster>> v1) throws Exception {
	GroupClusters result = new GroupClusters(initial_size);
	Iterator<ArrayList<Cluster>> itr = v1.iterator();
	while (itr.hasNext()) {
	    result.addCluster(itr.next());
	}
	result.validate();
	return result;
    }

}
