package partitioner;

import java.util.ArrayList;



import model.Cluster;

public class OverlapPartitionerTest {
    private static int L = 3;
    private static int start = 60;
    private static int end = 80;
    private static int num_of_pars = 4;

    private static int each_par_size = (int) Math.ceil(((end - start + 1) + L
	    * (num_of_pars - 1))
	    * 1.0 / num_of_pars);

    public static ArrayList<Integer> call(Cluster c) throws Exception {
	ArrayList<Integer> results = new ArrayList<>();
	int ts = c.getTS();
	// find the corresponding group cluster, and emit group-cluster pair  
	int index = ts / (each_par_size - L);
	int remain = ts - index * (each_par_size - L);
	int prev_index = -1;
	int next_index = -1;
	if (remain < L) {
	    if (index != 0) {
		prev_index = index - 1;
	    }
	} else if (each_par_size - remain < L) {
	    if (index != num_of_pars - 1) {
		next_index = index + 1;
	    }
	}
	results.add(index);
	if (prev_index != -1) {
	    results.add(prev_index);
	}
	if (next_index != -1) {
	    results.add(next_index);
	}
	return results;
    }

    public static void main(String[] args) {
	ArrayList<Cluster> input = new ArrayList<>();
	for (int i = start; i <= end; i++) {
	    input.add(new Cluster(i, "C" + i));
	}
	// OverlapPartitioner olp = new OverlapPartitioner(0, 19, 5, 3);
	try {
	    for (Cluster in : input) {
		System.out.print(in.getTS() + "\t");
		for (Integer  m: call(in)) {
		    // System.out.println(m._1 + "\t" + m._2);
		    System.out.print(m + "\t");
		}
		System.out.println();
	    }
 
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }
}
