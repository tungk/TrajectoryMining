package twophasejoin;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import model.Cluster;
import model.Pattern;
import model.SnapShot;
import olpartitioned.SnapshotCombinator;
import olpartitioned.SnapshotGenerator;
import olpartitioned.TupleFilter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import common.SerializableComparator;

import conf.AppProperties;

import scala.Tuple2;

public class TwoPhaseLayout implements Serializable {
    private static final long serialVersionUID = 7409346004803197432L;
    private JavaRDD<String> inputs;
    private int k, l, m, g;

    public TwoPhaseLayout(JavaRDD<String> textFile) {
	inputs = textFile;
	k = Integer.parseInt(AppProperties.getProperty("K"));
	l = Integer.parseInt(AppProperties.getProperty("L"));
	m = Integer.parseInt(AppProperties.getProperty("M"));
	g = Integer.parseInt(AppProperties.getProperty("G"));
    }

    /**
     * Use a two phase logic to
     */
    public void runLogic() {
	System.out.println("Start:\t" + k + "\t" + l + "\t" + m + "\t" + g);
	// DBSCAN first
	JavaPairRDD<Integer, SnapShot> TS_CLUSTERS = inputs
		.filter(new TupleFilter())
		.mapToPair(new SnapshotGenerator())
		.reduceByKey(new SnapshotCombinator(),
			conf.Constants.SNAPSHOT_PARTITIONS);
	// System.out.println(TS_CLUSTERS.count());
	// DBSCAN
	JavaPairRDD<Integer, ArrayList<Cluster>> CLUSTERS = TS_CLUSTERS
		.mapValues(new DBSCANWrapper(conf.Constants.EPS,
			conf.Constants.MINPTS));

	JavaRDD<ArrayList<Cluster>> ALLCLUSTERS = CLUSTERS
		.map(new Function<Tuple2<Integer, ArrayList<Cluster>>, ArrayList<Cluster>>() {
		    private static final long serialVersionUID = -395064130706525285L;

		    @Override
		    public ArrayList<Cluster> call(
			    Tuple2<Integer, ArrayList<Cluster>> v1)
			    throws Exception {
			ArrayList<Cluster> result = new ArrayList<Cluster>();
			for (Cluster c : v1._2) {
			    if (c.getObjects().size() >= m) {
				result.add(c);
			    }
			}
			return result;
		    }
		});
	System.out.println(ALLCLUSTERS.count());
	// we then use G-overlapped partition to filter clusters
	Integer max = TS_CLUSTERS
		.max(new SerializableComparator<Tuple2<Integer, SnapShot>>() {
		    private static final long serialVersionUID = -8648755182077751659L;

		    @Override
		    public int compare(Tuple2<Integer, SnapShot> o1,
			    Tuple2<Integer, SnapShot> o2) {
			return o1._1 - o2._1;
		    }
		})._1;

	Integer min = TS_CLUSTERS
		.min(new SerializableComparator<Tuple2<Integer, SnapShot>>() {
		    private static final long serialVersionUID = 5901382262644876036L;

		    @Override
		    public int compare(Tuple2<Integer, SnapShot> o1,
			    Tuple2<Integer, SnapShot> o2) {
			return o1._1 - o2._1;
		    }
		})._1;
	System.out.println("Max: " + max + "\tMin: " + min);
	JavaRDD<Cluster> result1 = ALLCLUSTERS
		.flatMap(new FlatMapFunction<ArrayList<Cluster>, Cluster>() {
		    private static final long serialVersionUID = -1356115407860530179L;

		    @Override
		    public Iterable<Cluster> call(ArrayList<Cluster> t)
			    throws Exception {
			return t;
		    }

		});
	System.out.println("Initial Clusters:\t" + result1.count());
	// JavaRDD<Pattern> result =
	JavaRDD<Cluster> result2 = ALLCLUSTERS
		.flatMapToPair(new OverlapPartitioner(min, max, 31, g))
		.groupByKey()
		.mapValues(new CMCFilter(m, g))
		.flatMap(
			new FlatMapFunction<Tuple2<Integer, ArrayList<Cluster>>, Cluster>() {
			    private static final long serialVersionUID = -7121747873874627614L;

			    @Override
			    public Iterable<Cluster> call(
				    Tuple2<Integer, ArrayList<Cluster>> t)
				    throws Exception {
				return t._2;
			    }
			});
	List<Cluster> rr = result2.collect();
	if(rr.size() < 10) {
	    for(Cluster c : rr) {
		System.out.println(c.getObjects().size());
	    }
	}
	System.out.println("Secondary Clusters:\t" + rr.size());

	JavaPairRDD<Cluster, Integer> result3 = result2
		.flatMapToPair(
			new PairFlatMapFunction<Cluster, Cluster, Integer>() {
			    private static final long serialVersionUID = -6856165077653776200L;

			    @Override
			    public Iterable<Tuple2<Cluster, Integer>> call(
				    Cluster t) throws Exception {
				ArrayList<Tuple2<Cluster, Integer>> results = new ArrayList<>();
				HashSet<ArrayList<Integer>> combinations = new HashSet<>();
				ClusterCombinator.genCombination(
					new ArrayList<Integer>(t.getObjects()),
					new ArrayList<Integer>(), 0, m,
					combinations);
				for (ArrayList<Integer> combination : combinations) {
				    Cluster c2 = new Cluster(t.getContext());
				    c2.addAllObjects(combination);
				    results.add(new Tuple2<Cluster, Integer>(
					    c2, t.getTS()));
				}
				return results;
			    }
			}).groupByKey()
		.mapValues(new Function<Iterable<Integer>, Integer>() {
		    private static final long serialVersionUID = 9024904985342096408L;

		    @Override
		    public Integer call(Iterable<Integer> v1) throws Exception {
			int count = 0;
			Iterator<Integer> itr = v1.iterator();
			while (itr.hasNext()) {
			    count++;
			    itr.next();
			}
			return count;
		    }
		});
	System.out.println("Combinations Generated:\t" + result3.count());
	// flatMapToPair(
	// new PairFlatMapFunction<Tuple2<Integer, ArrayList<Cluster>>, Cluster,
	// Integer>() {
	// private static final long serialVersionUID = -3424090995370901210L;
	//
	// @Override
	// public Iterable<Tuple2<Cluster, Integer>> call(
	// Tuple2<Integer, ArrayList<Cluster>> t)
	// throws Exception {
	// ArrayList<Tuple2<Cluster, Integer>> result = new ArrayList<>();
	// for (Cluster c : t._2) {
	// HashSet<ArrayList<Integer>> combinations = new HashSet<>();
	// ClusterCombinator.genCombination(
	// new ArrayList<Integer>(c
	// .getObjects()),
	// new ArrayList<Integer>(), 0, m,
	// combinations);
	// for (ArrayList<Integer> combination : combinations) {
	// Cluster c2 = new Cluster(c.getContext());
	// c2.addAllObjects(combination);
	// result.add(new Tuple2<Cluster, Integer>(
	// c2, c.getTS()));
	// }
	// }
	// return result;
	// }
	// })
	// .groupByKey()
	// .mapValues(
	// new Function<Iterable<Integer>, ArrayList<ArrayList<Integer>>>() {
	// private static final long serialVersionUID = -7245979451658206130L;
	//
	// @Override
	// public ArrayList<ArrayList<Integer>> call(
	// Iterable<Integer> v1) throws Exception {
	// ArrayList<Integer> input = new ArrayList<>();
	// // add v1 to input set;
	// for (Integer i : v1) {
	// input.add(i);
	// }
	// // sort input
	// Collections.sort(input);
	// return GenValidTemporal
	// .genValid(input, l, k, g);
	// }
	// })
	// .flatMap(
	// new FlatMapFunction<Tuple2<Cluster, ArrayList<ArrayList<Integer>>>,
	// Pattern>() {
	// private static final long serialVersionUID = 3780592154329905114L;
	//
	// @Override
	// public Iterable<Pattern> call(
	// Tuple2<Cluster, ArrayList<ArrayList<Integer>>> t)
	// throws Exception {
	// ArrayList<Pattern> results = new ArrayList<>();
	// Cluster c = t._1;
	// ArrayList<ArrayList<Integer>> temporals = t._2;
	// for (ArrayList<Integer> temp : temporals) {
	// results.add(new Pattern(c.getObjects(),
	// temp));
	// }
	// return results;
	// }
	// }); // no need to filter on the empty ones;

	System.out.println("finished!");
	// List<Pattern> patterns = result.collect();
	// for(Pattern p : patterns ) {
	// System.out.println(p);
	// }
    }
}
