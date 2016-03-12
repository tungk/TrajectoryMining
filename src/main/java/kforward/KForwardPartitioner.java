package kforward;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * partition a series of timestamp [min,max] into overlapped partitions.
 * @author a0048267
 *
 */
public class KForwardPartitioner implements
	PairFlatMapFunction<SnapshotCluster, Integer, SnapshotCluster> {
    private static final long serialVersionUID = -5332024402601915234L;

    private int overlaps;
    private int min;
    private int par_length;
    
    public KForwardPartitioner(int min, int max, int overlaps, int partitions) {
	this.min = min;
	this.overlaps = overlaps;
	par_length = (int) Math.ceil((max-min+1)*1.0/partitions);
    }
    
    
    @Override
    public Iterable<Tuple2<Integer, SnapshotCluster>> call(SnapshotCluster t)
	    throws Exception {
	ArrayList<Tuple2<Integer, SnapshotCluster>> tmp = new ArrayList<>();
	int time = t.getTS();
	ArrayList<Integer> keys = computePartitionID(time);
	for(int key : keys) {
	    tmp.add(new Tuple2<Integer, SnapshotCluster>(key, t));
	}
	return tmp;
    }
    
    /**
     * given a time, find its position in the partitions 
     * @param time
     * @return
     */
    private ArrayList<Integer> computePartitionID(int time) {
	ArrayList<Integer> indexes = new ArrayList<>();
	time = time - min; // make a shift
	int first_index = time/par_length;
	indexes.add(first_index);
	int nearest_split_point = time/par_length * par_length;
	while(time - nearest_split_point < overlaps) {
	    if(first_index > 0) {
		indexes.add(--first_index);
		nearest_split_point = nearest_split_point - par_length;
	    } else {
		break;
	    }
	}
	return indexes;
    }
    
    public static void main(String[] args) {
	int min = 2;
	int max = 2879;
	int par = 23;
	int overlap = 30;
	KForwardPartitioner kfp = new KForwardPartitioner(min, max, overlap, par);
	System.out.println(kfp.par_length);
	for(int i = min; i <= max; i++) {
	   System.out.print(i+":\t");
	   ArrayList<Integer> keys = kfp.computePartitionID(i);
	   for(int key : keys) {
	       System.out.print(key+"\t");
	   }
	   System.out.println();
	}
    }
}
