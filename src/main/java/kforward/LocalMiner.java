package kforward;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import kforward.SetComp.Result;

public class LocalMiner implements Serializable{
    private static final long serialVersionUID = 416636556315652980L;
    private int K, M, L, G;

    public LocalMiner(int k, int m, int l, int g) {
	super();
	K = k;
	M = m;
	L = l;
	G = g;
    }

    private ArrayList<ArrayList<SimpleCluster>> input;

    public void setInput(ArrayList<ArrayList<SimpleCluster>> input) {
	this.input = input;
    }

    public ArrayList<HashSet<Integer>> mining() {
	ArrayList<HashSet<Integer>> result = new ArrayList<>();
	if (input.size() < L) {
	    return result; // not enough inputs
	}
	ArrayList<Pattern> candidates = new ArrayList<>();
	HashSet<HashSet<Integer>> obj_index = new HashSet<>();
	// insert clusters at snapshot 1 to be the pattern
	ArrayList<SimpleCluster> initial_snapshot = input.get(0);
	for (SimpleCluster cluster : initial_snapshot) {
	    if (cluster.getSize() >= M) {
		// significant
		Pattern p = new Pattern(M, L, K, G);
		p.addObjects(cluster.getObjects());
		p.growTemporal(cluster.getTS());
		candidates.add(p);
		obj_index.add(p.getObjects());
	    }
	}
	int end = input.size();
	for (int i = 1; i < end; i++) {
	    ArrayList<SimpleCluster> current_snap = input.get(i);
	    int current_ts = current_snap.get(0).getTS();
	    // join current_snapshots with candidate set to see
	    // any chances to form a new pattern
	    for (SimpleCluster cluster : current_snap) {
		// since clusters are disjoint, checking every candidate
		boolean create_single_pattern = true;
		for (int k = 0, can_size = candidates.size(); k < can_size; k++) {
		    Pattern p = candidates.get(k);
		    SetComp comp = SetUtils.compareSets(cluster.getObjects(),
			    p.getObjects());
		    if (comp.getResult() == Result.SUPER
			    || comp.getResult() == Result.EQUAL) {
			// grow the current pattern
			if (!p.growTemporal(current_ts)) {
			    Pattern np = new Pattern(M, L, K, G);
			    np.addObjects(p.getObjects());
			    np.growTemporal(current_ts);
			    candidates.add(np);
			}
			create_single_pattern = (comp.getResult() != Result.EQUAL);
		    } else if (comp.getResult() == Result.SUB) {
			Set<Integer> commons = comp.getIntersect();
			if (commons.size() >= M && !obj_index.contains(commons)) {
			    // create a new pattern;
			    Pattern newp = new Pattern(M, L, K, G);
			    newp.addObjects(commons);
			    newp.addTemporals(p.getTstamps());
			    if (!newp.growTemporal(current_ts)) {
				newp.clearTemporals();
				newp.growTemporal(current_ts);
			    }
			    candidates.add(newp);
			    obj_index.add(newp.getObjects());
			}
		    } else {
			continue;
		    }
		}
		if (create_single_pattern) {
		    if (obj_index.contains(cluster)) {
			continue;
		    }
		    Pattern newp = new Pattern(M, L, K, G);
		    newp.addObjects(cluster.getObjects());
		    newp.growTemporal(current_ts); // a single pattern always succeed
		    candidates.add(newp);
		    obj_index.add(newp.getObjects());
		}
	    }
	    // further remove unqualified patterns
	    Iterator<Pattern> pitr = candidates.iterator();
	    while (pitr.hasNext()) {
		Pattern p = pitr.next();
		if (i - p.getLatestTS() >= G) { // checking for expiring, here
						// we use >=, since in the next
						// round, every cluster has one
						// more temporal element, then
						// >= becomes > automatically
		    pitr.remove();
		    if (p.checkFullValidity()) {
			result.add(p.getObjects());
		    } else {
			// remove the pattern only if it is expired && it is not
			// valid
			obj_index.remove(p.getObjects());
		    }
		}
	    }
	}

	// dump the patterns in the final cache
	Iterator<Pattern> pitr = candidates.iterator();
	while (pitr.hasNext()) {
	    Pattern p = pitr.next();
	    //TODO:: remove when final deploy
	    System.out.println(p);
	    if (p.checkFullValidity()) {
		result.add(p.getObjects());
	    }
	}
	return result;
    }

    public static void main(String[] args) {
	int K = 5;
	int L = 1;
	int G = 5;
	int M = 3;
	LocalMiner lm = new LocalMiner(K, M, L, G);
	ArrayList<ArrayList<SimpleCluster>> input = new ArrayList<>();
	ArrayList<SimpleCluster> sp1 = new ArrayList<>();
	ArrayList<SimpleCluster> sp2 = new ArrayList<>();
	ArrayList<SimpleCluster> sp3 = new ArrayList<>();
	ArrayList<SimpleCluster> sp4 = new ArrayList<>();
	ArrayList<SimpleCluster> sp5 = new ArrayList<>();
	ArrayList<SimpleCluster> sp6 = new ArrayList<>();
	ArrayList<SimpleCluster> sp7 = new ArrayList<>();
	ArrayList<SimpleCluster> sp8 = new ArrayList<>();
	ArrayList<SimpleCluster> sp9 = new ArrayList<>();
	ArrayList<SimpleCluster> sp10 = new ArrayList<>();
	sp1.add(new SimpleCluster(2, Arrays.asList(1, 2, 3)));
	sp1.add(new SimpleCluster(2, Arrays.asList(4, 5, 6)));
	sp1.add(new SimpleCluster(2, Arrays.asList(7, 8, 9)));
	input.add(sp1);

	sp2.add(new SimpleCluster(3, Arrays.asList(1, 2, 3, 4)));
	sp2.add(new SimpleCluster(3, Arrays.asList(5, 6, 7, 8, 9)));
	input.add(sp2);

	sp3.add(new SimpleCluster(4, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9)));
	input.add(sp3);

	sp4.add(new SimpleCluster(5, Arrays.asList(1, 2, 4)));
	sp4.add(new SimpleCluster(5, Arrays.asList(3, 5, 6)));
	sp4.add(new SimpleCluster(5, Arrays.asList(7, 8, 9)));
	input.add(sp4);

	sp5.add(new SimpleCluster(6, Arrays.asList(1, 2, 7, 8, 9)));
	sp5.add(new SimpleCluster(6, Arrays.asList(4, 5, 6)));
	sp5.add(new SimpleCluster(6, Arrays.asList(3)));
	input.add(sp5);

	sp6.add(new SimpleCluster(7, Arrays.asList(1, 2, 3, 4)));
	sp6.add(new SimpleCluster(7, Arrays.asList(5, 6, 7)));
	sp6.add(new SimpleCluster(7, Arrays.asList(8, 9)));
	input.add(sp6);

	sp7.add(new SimpleCluster(8, Arrays.asList(1, 8, 9)));
	sp7.add(new SimpleCluster(8, Arrays.asList(2, 3, 4)));
	sp7.add(new SimpleCluster(8, Arrays.asList(5, 6, 7)));
	input.add(sp7);

	sp8.add(new SimpleCluster(9, Arrays.asList(1, 2, 3, 4)));
	sp8.add(new SimpleCluster(9, Arrays.asList(5, 6)));
	sp8.add(new SimpleCluster(9, Arrays.asList(7, 8, 9)));
	input.add(sp8);

	sp9.add(new SimpleCluster(10, Arrays.asList(1, 2, 3)));
	sp9.add(new SimpleCluster(10, Arrays.asList(4, 5, 6)));
	sp9.add(new SimpleCluster(10, Arrays.asList(7, 8, 9)));
	input.add(sp9);

	sp10.add(new SimpleCluster(11, Arrays.asList(1, 2, 3)));
	sp10.add(new SimpleCluster(11, Arrays.asList(4, 5, 8)));
	sp10.add(new SimpleCluster(11, Arrays.asList(6, 7, 9)));
	input.add(sp10);

	for (ArrayList<SimpleCluster> in : input) {
	    System.out.println(in);
	}

	lm.setInput(input);
	ArrayList<HashSet<Integer>> result = lm.mining();

	int index = 0;
	for (HashSet<Integer> cluster : result) {
	    System.out.println((index++) + "\t" + cluster);
	}
    }
    /**
     * clear the current status of the lm
     */
    public void clear() {
    }
}
