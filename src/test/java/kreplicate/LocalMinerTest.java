package kreplicate;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import model.SimpleCluster;
import model.SnapshotClusters;

public class LocalMinerTest {
    public static void main(String[] args) {
	ArrayList<SnapshotClusters> input =loadInput("C:/Users/a0048267/Desktop/input.txt");
//	for(SnapshotClusters sc : input) {
//	    System.out.println(sc);
//	}
	int K = 20, G = 2, L=5, M = 20; 
	LocalMiner lm = new LocalMiner(K, M, L, G);
	try {
	    lm.call(input);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    private static ArrayList<SnapshotClusters> loadInput(String file) {
	FileReader fr;
	BufferedReader br;
	ArrayList<SnapshotClusters> input = new ArrayList<>();
	
	try {
	    fr = new FileReader(file);
	    br = new BufferedReader(fr);
	    //read file line by line;
	    
	    String line;
	    String[] splits;
	    String[] secondary_splits;
	    while((line = br.readLine()) != null) {
		if(line.startsWith("#")) {
		    continue;
		} else {
		    splits = line.split("\t");
		    if(splits.length < 2) {
			continue;
		    } else {
			int ts = Integer.parseInt(splits[0]);
		    	SnapshotClusters snapshot = new SnapshotClusters(ts);
		    	for(int j = 1; j < splits.length; j++) {
		    	    secondary_splits = splits[j].split("-");
		    	    SimpleCluster cluster = new SimpleCluster();
		    	    String[] objs = secondary_splits[1].split(", ");
		    	    for(int k = 0; k < objs.length; k++) {
		    		cluster.addObject(Integer.parseInt(objs[k]));
		    	    }
		    	    cluster.setID(secondary_splits[0]);
		    	    snapshot.addCluster(cluster);
		    	}
		    	input.add(snapshot);
		    }
		}
	    }
	    br.close();
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return input;
    }
}
