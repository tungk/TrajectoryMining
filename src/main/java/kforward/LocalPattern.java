package kforward;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;

import model.SnapshotClusters;

import org.apache.spark.api.java.function.Function;

/**
 * mining local patterns based on the given set of snapshot clusters
 * @author a0048267
 * 
 */
public class LocalPattern implements Function<Iterable<SnapshotClusters>, Iterable<HashSet<Integer>>>{
    private static final long serialVersionUID = 7626346274874868740L;
//    private int K, L,G,M;
    private LocalMiner lm;
    public LocalPattern(int L, int K, int M, int G) {
//	this.K = K;
//	this.L = L;
//	this.M = M;
//	this.G = G;
	lm = new LocalMiner(K,M,L,G); 
    }
    
    @Override
    public Iterable<HashSet<Integer>> call(Iterable<SnapshotClusters> v1)
	    throws Exception { 
	ArrayList<SnapshotClusters> tmp = new ArrayList<>();
	Iterator<SnapshotClusters> itr = v1.iterator();
	ArrayList<HashSet<Integer>> patterns = new ArrayList<>();
	
	while(itr.hasNext()) {
	    tmp.add(itr.next());
	}
	 
	//TODO:: not need to sort
//	Collections.sort(tmp, new SimpleClusterListComparator());
	//TODO:: remove when final deploy
	System.out.println("input:" + tmp);
	//at this point, the tmp list is sorted, use CMC to 
	//find the clusters which contains patterns
	//scan the snaphots from first to the end;
	lm.clear();
	lm.setInput(tmp);
	patterns = lm.mining();
	return patterns;
    }
    
    /**
     * a unit-test
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
//	ArrayList<ArrayList<SimpleCluster>> input = new ArrayList<>();
//	ArrayList<SimpleCluster> sp1 = new ArrayList<>();
//	ArrayList<SimpleCluster> sp2 = new ArrayList<>();
//	ArrayList<SimpleCluster> sp3 = new ArrayList<>();
//	ArrayList<SimpleCluster> sp4 = new ArrayList<>();
//	ArrayList<SimpleCluster> sp5 = new ArrayList<>();
//	ArrayList<SimpleCluster> sp6 = new ArrayList<>();
//	ArrayList<SimpleCluster> sp7 = new ArrayList<>();
//	ArrayList<SimpleCluster> sp8 = new ArrayList<>();
//	ArrayList<SimpleCluster> sp9 = new ArrayList<>();
//	ArrayList<SimpleCluster> sp10 = new ArrayList<>();
//	sp1.add(new SimpleCluster(2, Arrays.asList(1, 2, 3)));
//	sp1.add(new SimpleCluster(2, Arrays.asList(4, 5, 6)));
//	sp1.add(new SimpleCluster(2, Arrays.asList(7, 8, 9)));
//	input.add(sp1);
//
//	sp2.add(new SimpleCluster(3, Arrays.asList(1, 2, 3, 4)));
//	sp2.add(new SimpleCluster(3, Arrays.asList(5, 6, 7, 8, 9)));
//	input.add(sp2);
//
//	sp3.add(new SimpleCluster(4, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9)));
//	input.add(sp3);
//
//	sp4.add(new SimpleCluster(5, Arrays.asList(1, 2, 4)));
//	sp4.add(new SimpleCluster(5, Arrays.asList(3, 5, 6)));
//	sp4.add(new SimpleCluster(5, Arrays.asList(7, 8, 9)));
//	input.add(sp4);
//
//	sp5.add(new SimpleCluster(6, Arrays.asList(1, 2, 7, 8, 9)));
//	sp5.add(new SimpleCluster(6, Arrays.asList(4, 5, 6)));
//	sp5.add(new SimpleCluster(6, Arrays.asList(3)));
//	input.add(sp5);
//
//	sp6.add(new SimpleCluster(7, Arrays.asList(1, 2, 3, 4)));
//	sp6.add(new SimpleCluster(7, Arrays.asList(5, 6, 7)));
//	sp6.add(new SimpleCluster(7, Arrays.asList(8, 9)));
//	input.add(sp6);
//
//	sp7.add(new SimpleCluster(8, Arrays.asList(1, 8, 9)));
//	sp7.add(new SimpleCluster(8, Arrays.asList(2, 3, 4)));
//	sp7.add(new SimpleCluster(8, Arrays.asList(5, 6, 7)));
//	input.add(sp7);
//
//	sp8.add(new SimpleCluster(9, Arrays.asList(1, 2, 3, 4)));
//	sp8.add(new SimpleCluster(9, Arrays.asList(5, 6)));
//	sp8.add(new SimpleCluster(9, Arrays.asList(7, 8, 9)));
//	input.add(sp8);
//
//	sp9.add(new SimpleCluster(10, Arrays.asList(1, 2, 3)));
//	sp9.add(new SimpleCluster(10, Arrays.asList(4, 5, 6)));
//	sp9.add(new SimpleCluster(10, Arrays.asList(7, 8, 9)));
//	input.add(sp9);
//
//	sp10.add(new SimpleCluster(11, Arrays.asList(1, 2, 3)));
//	sp10.add(new SimpleCluster(11, Arrays.asList(4, 5, 8)));
//	sp10.add(new SimpleCluster(11, Arrays.asList(6, 7, 9)));
//	input.add(sp10);
//	LocalPattern lp = new LocalPattern(1,5,3,5);
//	for(HashSet<Integer> pattern : lp.call(input)) {
//	    System.out.println(pattern);
//	}
    }
}

class SimpleClusterListComparator implements Comparator<SnapshotClusters>, Serializable {
    private static final long serialVersionUID = -7921743971973093960L;
    @Override
    public int compare(SnapshotClusters o1, SnapshotClusters o2) {
	return o1.getTimeStamp() - o2.getTimeStamp();
    }
}
