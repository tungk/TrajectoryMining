package apriori;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.spark.Partitioner;

public class TempPartitioner extends Partitioner implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -4896673433908039584L;
    
    private int num_pars;
    private HashMap<Integer,Integer> id_bucket_mapping;
  
    public TempPartitioner(int clique_partitions, final HashMap<Integer, Integer> mappings) {
	num_pars = clique_partitions;
	id_bucket_mapping = mappings;
    }

    @Override
    public int getPartition(Object arg0) {
	if(arg0 instanceof Integer) {
	    return id_bucket_mapping.get((int)arg0);
	} else {
	    return 0;
	}
    }

    @Override
    public int numPartitions() {
	return num_pars;
    }
}
