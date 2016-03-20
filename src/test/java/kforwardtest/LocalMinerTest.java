package kforwardtest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;

import kforward.LocalMiner;
import kforward.SimpleCluster;

  
public class LocalMinerTest {
   /**
    * testing the {@link LocalMiner.java} with crawled inputs from NEX
    * @param args
    */
    public static void main(String[] args) throws Exception {
	//prepare input for lm
	
	int K = 50, G = 2, L = 10, M = 10;
	LocalMiner lm = new LocalMiner(K, M, L, G);
	
	String local_input = "C:/Users/a0048267/Desktop/input.txt";
	FileReader fr = new FileReader(local_input);
	BufferedReader br = new BufferedReader(fr);
	String line;
	ArrayList<ArrayList<SimpleCluster>> input = new ArrayList<>();
	while((line = br.readLine()) != null) {
//	    SimpleCluster sc = new SimpleCluster();
	    //every line is a snapshot
	    ArrayList<SimpleCluster> current_snapshot = new ArrayList<>();
	    String[] parts = line.split("\t");
	    for(int i = 0; i < parts.length; i++) {
		String[] idtsoid = parts[i].split(":");
		String[] tsoids = idtsoid[1].split(",");
		int ts = Integer.parseInt(tsoids[0]);
		SimpleCluster sc = new SimpleCluster(ts, idtsoid[0]);
		for(int j = 1; j < tsoids.length; j++) {
		    sc.addObject(Integer.parseInt(tsoids[j]));
		}
		current_snapshot.add(sc);
	    }
	    input.add(current_snapshot);
	}
	br.close();
	
	lm.setInput(input);
	
	ArrayList<HashSet<Integer>> result = lm.mining();
	int i = 0;
	for(HashSet<Integer> cluster : result) {
	    System.out.println("Cluster " + i+":\t"+ cluster);
	    i++;
	}
    }
}
