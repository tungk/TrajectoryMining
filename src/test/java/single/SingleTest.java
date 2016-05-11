package single;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import apriori.CliqueMiner;

import scala.Tuple2;

import model.SimpleCluster;
import model.SnapshotClusters;

public class SingleTest {
    public static void main(String[] args) {
	// prepare input from file
	String filename = "C:/Users/a0048267/Desktop/input4.txt";
	try {
	    FileReader fr = new FileReader(filename);
	    BufferedReader br = new BufferedReader(fr);
	    String line;
	    ArrayList<SnapshotClusters> input = new ArrayList<>();
	    while ((line = br.readLine()) != null) {
		String[] parts = line.split("-");
		int time_stamp = Integer.parseInt(parts[0]);
		SnapshotClusters sc = new SnapshotClusters(time_stamp);
		String[] parts2 = parts[1].split("\\|");
		SimpleCluster cluster = new SimpleCluster();
		cluster.setID(parts2[0]);
		for(String obj : parts2[1].split(",")) {
		    cluster.addObject(Integer.parseInt(obj));
		}
		sc.addCluster(cluster);
		input.add(sc);
	    }
	    br.close();
	    for (SnapshotClusters out : input) {
		System.out.println(out);
	    }
	    SinglePattern sp = new SinglePattern(null, 3, 1, 10,1);
	    ArrayList<IntSet>result = sp.localMiner(input, 1, 20);
	    System.out.println(result);
	    
	   HashMap<Integer,IntSortedSet> sets = new HashMap<Integer,IntSortedSet>();
	    for(int i = 0; i < 10; i++) {
		SnapshotClusters sc = input.get(i);
		SimpleCluster cluster = sc.getClusterAt(0);
		IntSet objset = cluster.getObjects();
		int k = 40;
		for(int obj : objset) {
		    if(!sets.containsKey(obj)) {
			sets.put(obj, new IntRBTreeSet());
		    } 
		    sets.get(obj).add(sc.getTimeStamp());
		}
	    }
	    ArrayList<Tuple2<Integer, IntSortedSet>> input_set = new ArrayList<>();
	    for(Entry<Integer, IntSortedSet> set : sets.entrySet()) {
		input_set.add(new Tuple2<Integer, IntSortedSet>(set.getKey(), set.getValue()));
	    }
	    
	    Tuple2<Integer, Iterable<Tuple2<Integer,IntSortedSet>>>
	    	cminput = new Tuple2<Integer, Iterable<Tuple2<Integer,IntSortedSet>>>(40, input_set);
	    System.out.println(cminput);
	    CliqueMiner cm = new CliqueMiner(10,3, 1,1);
	    for(IntSet is : cm.call(cminput)) {
		System.out.println(is);
	    }
	    
	} catch (IOException e) {
	    e.printStackTrace();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }
}
