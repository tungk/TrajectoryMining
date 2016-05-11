package apriori;

import java.io.Serializable;

import org.apache.spark.Partitioner;

public class TempPartitioner extends Partitioner implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -4896673433908039584L;
    
    private int num_pars;
  
    public TempPartitioner(int clique_partitions) {
	num_pars = clique_partitions;
    }

    @Override
    public int getPartition(Object arg0) {
	if(arg0 instanceof Integer) {
	    int key = (int) arg0;
	    return key % (num_pars);
	} else {
	    return 0;
	}
    }

    @Override
    public int numPartitions() {
	return num_pars;
    }

}
