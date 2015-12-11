package pattern;

import java.util.ArrayList;

import model.Cluster;
import model.GroupClusters;
import model.Pattern;
import model.SnapShot;

import baseline.LocalCMCMiner;

public class LocalCMCMinerTest {
    private static int L = 2, K = 3, G = 1, M = 2;
    
    public static void main(String[] args) {
	LocalCMCMiner lcmc = new LocalCMCMiner(M,L,G,K);
	GroupClusters input_gp = new GroupClusters(8);
	
	SnapShot sp= new SnapShot(0);
	ArrayList<Cluster> spc = new ArrayList<>();
	Cluster c1 = new Cluster(sp);
	Cluster c2 = new Cluster(sp);
	c1.addObject(3);c1.addObject(5);
	c2.addObject(1);c2.addObject(2); c2.addObject(4);  
	spc.add(c1); spc.add(c2);
	input_gp.addCluster(spc);
	
	sp = new SnapShot(1);
	spc = new ArrayList<>();
	c1 = new Cluster(sp);
	c2 = new Cluster(sp);
	c1.addObject(3);c1.addObject(5);
	c2.addObject(1);c2.addObject(2); c2.addObject(4);  
	spc.add(c1); spc.add(c2);
	input_gp.addCluster(spc);
	
	sp = new SnapShot(2);
	spc = new ArrayList<>();
	c1 = new Cluster(sp);
	c2 = new Cluster(sp);
	c1.addObject(1);c1.addObject(5);
	c2.addObject(2);c2.addObject(3); c2.addObject(4);  
	spc.add(c1); spc.add(c2);
	input_gp.addCluster(spc);
	
	
	sp = new SnapShot(3);
	spc = new ArrayList<>();
	c1 = new Cluster(sp);
	c2 = new Cluster(sp);
	c1.addObject(3);c1.addObject(5);
	c2.addObject(1);c2.addObject(2); c2.addObject(4);  
	spc.add(c1); spc.add(c2);
	input_gp.addCluster(spc);
	
	sp = new SnapShot(4);
	spc = new ArrayList<>();
	c1 = new Cluster(sp);
	c2 = new Cluster(sp);
	c1.addObject(1);c1.addObject(3);c1.addObject(5);
	c2.addObject(2); c2.addObject(4);  
	spc.add(c1); spc.add(c2);
	input_gp.addCluster(spc);
	
	sp = new SnapShot(5);
	spc = new ArrayList<>();
	c1 = new Cluster(sp);
	c2 = new Cluster(sp);
	c1.addObject(5);
	c2.addObject(2); c2.addObject(4);c2.addObject(3);c2.addObject(1);  
	spc.add(c1); spc.add(c2);
	input_gp.addCluster(spc);
	
	try {
	    ArrayList<Pattern> patterns =  lcmc.call(input_gp);
	    System.out.println("...");
	    for(Pattern p : patterns) {
		System.out.println(p);
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    } 
}
