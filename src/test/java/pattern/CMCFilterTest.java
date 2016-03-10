package pattern;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;

import model.Cluster;
import util.SetCompResult;

public class CMCFilterTest {
    public static int G = 1;
    public static int M = 2;
    public static ArrayList<Cluster> cmcFilter(ArrayList<ArrayList<Cluster>> v1) {
	ArrayList<ArrayList<Cluster>> tmp = new ArrayList<>();
	ArrayList<Cluster> result = new ArrayList<>();
	Iterator<ArrayList<Cluster>> itr = v1.iterator();
	while (itr.hasNext()) {
	    tmp.add(itr.next());
	}
	// sort the tmp using cluster timestamps;
	Collections.sort(tmp, new Comparator<ArrayList<Cluster>>() {
	    @Override
	    public int compare(ArrayList<Cluster> o1, ArrayList<Cluster> o2) {
		return o1.get(0).getTS() - o2.get(0).getTS();
	    }
	});
	// scan tmp list to find the local pattern
	for (int i = 0, len = tmp.size(); i < len; i++) {
	    HashSet<Cluster> tscurrent = new HashSet<>(tmp.get(i));
	    Iterator<Cluster> cluster_itr = tscurrent.iterator();
	    while (cluster_itr.hasNext()) {
		Cluster c = cluster_itr.next();
		boolean passed = false;
		for (int j = 1; j <= G && i + j < len; j++) {
	    	    passed = false;
		    HashSet<Cluster> tsnext = new HashSet<>(tmp.get(i + j));
		    for (Cluster n : tsnext) {
			SetCompResult sr = util.SetOps.setCompare(
				c.getObjects(), n.getObjects());
			if (sr.getCommonsSize() < M) {
			    // continue;
			} else {
			    passed = true;
			    break;
			}
		    }
		    if(!passed) {
			break;
		    }
		}
		if (!passed) {
		    // not proximate cluster within G-steps
		    cluster_itr.remove();
		} else {
		    result.add(c);
		}
	    }
	}
	return result;
    }
    
    public static void main(String[] args) {
	ArrayList<Cluster> c1 = new ArrayList<>();
	c1.add(new Cluster(1, "a", new int[]{1,2,3}));
	c1.add(new Cluster(1, "b", new int[]{4,5}));
	c1.add(new Cluster(1, "c", new int[]{7,8,9}));
	c1.add(new Cluster(1, "d", new int[]{10,11,12}));
	ArrayList<Cluster> c2 = new ArrayList<>();
	c2.add(new Cluster(2, "e", new int[]{1,2,4}));
	c2.add(new Cluster(2, "f", new int[]{3,5,6}));
	c2.add(new Cluster(2, "g", new int[]{7,8,9}));
	c2.add(new Cluster(2, "h", new int[]{10,11,12}));
	ArrayList<Cluster> c3 = new ArrayList<>();
	c3.add(new Cluster(3, "i", new int[]{1,2,3}));
	c3.add(new Cluster(3, "j", new int[]{4,5}));
	c3.add(new Cluster(3, "k", new int[]{7,8,9}));
	c3.add(new Cluster(3, "l", new int[]{10,11,12}));
	ArrayList<Cluster> c4 = new ArrayList<>();
	c4.add(new Cluster(4, "m", new int[]{1,2,3}));
	c4.add(new Cluster(4, "n", new int[]{4,5,6}));
	c4.add(new Cluster(4, "o", new int[]{7,8,9}));
	c4.add(new Cluster(4, "p", new int[]{10,11,12}));
	
	ArrayList<ArrayList<Cluster>> raw = new ArrayList<>();
	raw.add(c1);
	raw.add(c2);
	raw.add(c3);
	raw.add(c4);
	System.out.println("CMC");
	ArrayList<Cluster> result = cmcFilter(raw);
	for(Cluster c : result) {
	    System.out.println(c);
	}
    }
}
