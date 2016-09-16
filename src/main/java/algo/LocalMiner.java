package algo;

/**
 * THIS SHOULD NOT BE RUN IN SPARK-1.6.2 or 1.5.2
 * In Spark 2.0.0, please uncomment the following
*/

/*

import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import model.SelfAdjustPattern;
import model.SnapshotClusters;
import model.SimpleCluster;

import org.apache.spark.api.java.function.Function;

import util.SetComp;
import util.SetUtils;
import util.SetComp.Result;

import common.SerializableComparator;

public class LocalMiner implements
		Function<Iterable<SnapshotClusters>, ArrayList<IntSet>> {
	private static final long serialVersionUID = 416636556315652980L;
	private int K, M, L, G;

	public LocalMiner(int k, int m, int l, int g) {
		K = k;
		M = m;
		L = l;
		G = g;
	}

	private ArrayList<SnapshotClusters> input;

	@Override
	public ArrayList<IntSet> call(Iterable<SnapshotClusters> v1)
			throws Exception {
		input = new ArrayList<SnapshotClusters>();
		Iterator<SnapshotClusters> v1itr = v1.iterator();
		long total_clusters = 0; // for statistics
		while (v1itr.hasNext()) {
			SnapshotClusters s = v1itr.next();
			input.add(s); // seems unnecessary copying action, but we have no
							// choice right now..
			total_clusters += s.getClusterSize();
		}
		Collections.sort(input, new SerializableComparator<SnapshotClusters>() {
			private static final long serialVersionUID = 321716012475853207L;

			@Override
			public int compare(SnapshotClusters o1, SnapshotClusters o2) {
				return o1.getTimeStamp() - o2.getTimeStamp();
			}
		});

		// TODO:: this checking should be removed in the final release
		boolean sorted = true;
		for (int i = 1; i < input.size(); i++) {
			if (input.get(i).getTimeStamp() - input.get(i - 1).getTimeStamp() <= 0) {
				sorted = false;
				break;
			}
		}
		if (sorted) {
			System.out.println("Input is sorted! ");
		} else {
			System.out.println("[ERROR] Input is not sorted!!");
		}
		// TODO:: delete till here
		// System.out.println("input:"+input);
		long start = System.currentTimeMillis();
		ArrayList<IntSet> mined_clusters = mining();
		System.out.println(total_clusters + " clusters in " + input.size()
				+ " timestamps, takes " + (System.currentTimeMillis() - start)
				+ " ms");
		return mined_clusters;
	}

	public ArrayList<IntSet> mining() {
		// long t_start, t_end;

		ArrayList<IntSet> result = new ArrayList<>();
		if (input.size() < L) {
			return result; // not enough inputs
		}
		ArrayList<SelfAdjustPattern> candidates = new ArrayList<>();
		HashSet<IntSet> obj_index = new HashSet<>();
		// insert clusters at snapshot 1 to be the pattern
		SnapshotClusters initial_sp = input.get(0);
		// t_start = System.currentTimeMillis();
		int ming_start_time = initial_sp.getTimeStamp();
		for (SimpleCluster cluster : initial_sp.getClusters()) {
			if (cluster.getSize() >= M) {
				// significant
				SelfAdjustPattern p = new SelfAdjustPattern(M, L, K, G);
				p.addObjects(cluster.getObjects());
				p.growTemporal(initial_sp.getTimeStamp());
				candidates.add(p);
			}
		}
		// t_end = System.currentTimeMillis();
		// System.out.println("Point 1:" + (t_end-t_start) +" ms");
		// ArrayList<SimpleCluster> initial_snapshot =
		// input.get(0).getClusters();
		// for (SimpleCluster cluster : initial_snapshot) {
		// if (cluster.getSize() >= M) {
		// // significant
		// SelfAdjustPattern p = new SelfAdjustPattern(M, L, K, G);
		// p.addObjects(cluster.getObjects());
		// p.growTemporal(cluster.getTS());
		// candidates.add(p);
		// }
		// }

		int end = input.size();
		// t_start = System.currentTimeMillis();
		// String p2 = "";
		for (int i = 1; i < end; i++) {
			// p2 += i+":"+candidates.size() +",";
			SnapshotClusters current_snap = input.get(i);
			int current_ts = current_snap.getTimeStamp();
			// join current_snapshots with candidate set to see
			// any chances to form a new pattern
			for (SimpleCluster cluster : current_snap.getClusters()) {
				// since clusters are disjoint, checking every candidate
				for (int k = 0, can_size = candidates.size(); k < can_size; k++) {
					SelfAdjustPattern p = candidates.get(k);
					SetComp comp = SetUtils.compareSets(cluster.getObjects(),
							p.getObjects());
					if (comp.getResult() == Result.SUPER
							|| comp.getResult() == Result.EQUAL) {
						// grow the current pattern
						if (!p.growTemporal(current_ts)
								&& !subsetOf(obj_index, p.getObjects())) {
							SelfAdjustPattern np = new SelfAdjustPattern(M, L,
									K, G);
							np.addObjects(p.getObjects());
							np.growTemporal(current_ts);
							candidates.add(np);
						}
					} else if (comp.getResult() == Result.SUB
							|| comp.getResult() == Result.NONE) {
						IntSet commons = comp.getIntersect();
						if (commons.size() >= M
								&& !subsetOf(obj_index, commons)) {
							// create a new pattern;
							SelfAdjustPattern newp = new SelfAdjustPattern(M,
									L, K, G);
							newp.addObjects(commons);
							newp.addTemporals(p.getTstamps());
							if (newp.growTemporal(current_ts)) {
								candidates.add(newp);
							}
						}
					}
				}
			}
			// further remove unqualified patterns
			Iterator<SelfAdjustPattern> pitr = candidates.iterator();
			while (pitr.hasNext()) {
				SelfAdjustPattern p = pitr.next();
				if (p.getEarlistTS() != ming_start_time) {
					pitr.remove();
					continue;
				}
				if (current_ts - p.getLatestTS() >= G) { // checking for
															// expiring, here
					// we use >=, since in the next
					// round, every cluster has one
					// more temporal element, then
					// >= becomes > automatically
					pitr.remove();
					if (p.checkFullValidity()) {
						if (!subsetOf(obj_index, p.getObjects())) {
							result.add(p.getObjects());
							addIndex(obj_index, p.getObjects());
						}
					}
				} else if (p.checkFullValidity()) {
					if (!subsetOf(obj_index, p.getObjects())) {
						// remove the qualified clusters as soon as possible
						result.add(p.getObjects());
						addIndex(obj_index, p.getObjects());
						pitr.remove();
					}
				}
			}
		}
		// dump the patterns in the final cache
		Iterator<SelfAdjustPattern> pitr = candidates.iterator();
		while (pitr.hasNext()) {
			SelfAdjustPattern p = pitr.next();
			if (p.checkFullValidity() && !subsetOf(obj_index, p.getObjects())) {
				result.add(p.getObjects());
				addIndex(obj_index, p.getObjects());
			}
		}
		// t_end = System.currentTimeMillis();
		// System.out.println("Point 2:" + (t_end-t_start) + " ms" + "," + p2);
		// for(HashSet<Integer> cluster : result) {
		// System.out.println(cluster);
		// }
		return result;
	}

	public static void main(String[] args) throws Exception {
		int K = 5;
		int L = 1;
		int G = 5;
		int M = 3;
		LocalMiner lm = new LocalMiner(K, M, L, G);
		ArrayList<SnapshotClusters> input = new ArrayList<>();
		SnapshotClusters sp1 = new SnapshotClusters(1);
		SnapshotClusters sp2 = new SnapshotClusters(2);
		SnapshotClusters sp3 = new SnapshotClusters(3);
		SnapshotClusters sp4 = new SnapshotClusters(4);
		SnapshotClusters sp5 = new SnapshotClusters(5);
		SnapshotClusters sp6 = new SnapshotClusters(6);
		SnapshotClusters sp7 = new SnapshotClusters(7);
		SnapshotClusters sp8 = new SnapshotClusters(8);
		SnapshotClusters sp9 = new SnapshotClusters(9);
		SnapshotClusters sp10 = new SnapshotClusters(10);
		sp1.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3)));
		sp1.addCluster(new SimpleCluster(Arrays.asList(4, 5, 6)));
		sp1.addCluster(new SimpleCluster(Arrays.asList(7, 8, 9)));

		sp2.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3, 4)));
		sp2.addCluster(new SimpleCluster(Arrays.asList(5, 6, 7, 8, 9)));

		sp3.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8,
				9)));

		sp4.addCluster(new SimpleCluster(Arrays.asList(1, 2, 4)));
		sp4.addCluster(new SimpleCluster(Arrays.asList(3, 5, 6)));
		sp4.addCluster(new SimpleCluster(Arrays.asList(7, 8, 9)));

		sp5.addCluster(new SimpleCluster(Arrays.asList(1, 2, 7, 8, 9)));
		sp5.addCluster(new SimpleCluster(Arrays.asList(4, 5, 6)));
		sp5.addCluster(new SimpleCluster(Arrays.asList(3)));

		sp6.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3, 4)));
		sp6.addCluster(new SimpleCluster(Arrays.asList(5, 6, 7)));
		sp6.addCluster(new SimpleCluster(Arrays.asList(8, 9)));

		sp7.addCluster(new SimpleCluster(Arrays.asList(1, 8, 9)));
		sp7.addCluster(new SimpleCluster(Arrays.asList(2, 3, 4)));
		sp7.addCluster(new SimpleCluster(Arrays.asList(5, 6, 7)));

		sp8.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3, 4)));
		sp8.addCluster(new SimpleCluster(Arrays.asList(5, 6)));
		sp8.addCluster(new SimpleCluster(Arrays.asList(7, 8, 9)));

		sp9.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3)));
		sp9.addCluster(new SimpleCluster(Arrays.asList(4, 5, 6)));
		sp9.addCluster(new SimpleCluster(Arrays.asList(7, 8, 9)));

		sp10.addCluster(new SimpleCluster(Arrays.asList(1, 2, 3)));
		sp10.addCluster(new SimpleCluster(Arrays.asList(4, 5, 8)));
		sp10.addCluster(new SimpleCluster(Arrays.asList(6, 7, 9)));

		input.add(sp1);
		input.add(sp2);
		input.add(sp3);
		input.add(sp4);
		input.add(sp5);
		input.add(sp6);
		input.add(sp7);
		input.add(sp8);
		input.add(sp9);
		input.add(sp10);

		for (SnapshotClusters in : input) {
			System.out.println(in);
		}

		ArrayList<IntSet> result = lm.call(input);

		int index = 0;
		for (IntSet cluster : result) {
			System.out.println((index++) + "\t" + cluster);
		}
	}

	/**
	 * clear the current status of the lm
	 */
	public void clear() {
	}

	private static boolean subsetOf(Iterable<IntSet> grounds, IntSet test) {
		for (IntSet ground : grounds) {
			if (ground.containsAll(test)) {
				return true;
			}
		}
		return false;
	}

	private static void addIndex(HashSet<IntSet> obj_index, IntSet objects) {
		Iterator<IntSet> index_itr = obj_index.iterator();
		boolean contained = false;
		while (index_itr.hasNext()) {
			IntSet index = index_itr.next();
			if (objects.containsAll(index)) {
				index_itr.remove();
			} else if (index.containsAll(objects)) {
				contained = true;
				break;
			}
		}
		if (!contained) {
			obj_index.add(objects);
		}
	}
}
*/