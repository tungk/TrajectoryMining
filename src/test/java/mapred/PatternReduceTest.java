package mapred;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import model.Pattern;

import org.apache.spark.api.java.function.Function2;

import util.SetCompResult;
import util.SetOps;
import util.TemporalVerification;

public class PatternReduceTest {
    private int L;
    private int G;
    private int K;
    private int M;

    public PatternReduceTest(int m, int l, int g, int k) {
	L = l;
	G = g;
	K = k;
	M = m;
    }

    public Function2<ArrayList<Pattern>, ArrayList<Pattern>, ArrayList<Pattern>> PAIRREDUCE = new Function2<ArrayList<Pattern>, ArrayList<Pattern>, ArrayList<Pattern>>() {
	private static final long serialVersionUID = 3382527097155623720L;
	// merge the two local patterns
	@Override
	public ArrayList<Pattern> call(ArrayList<Pattern> v1,
		ArrayList<Pattern> v2) throws Exception {
	    ArrayList<Pattern> result = new ArrayList<>();
	    HashSet<Pattern> excluded = new HashSet<Pattern>();
	    for (Pattern p1 : v1) {
		for (Pattern p2 : v2) {
		    if (p1.getEarlyTS() - p2.getLatestTS() > G) {
			continue;
		    }
		    if (p2.getEarlyTS() - p1.getLatestTS() > G) {
			continue; // no need to consider
		    }
		    Set<Integer> s1 = p1.getObjectSet();
		    Set<Integer> s2 = p2.getObjectSet();
		    SetCompResult scr = SetOps.setCompare(s1, s2);
		    // check common size
		    if (scr.getCommonsSize() < M) {
			// then the only pattern is p1 and p2
			continue;
		    }

		    if (scr.getStatus() == 1) {
			// p1 contains p2, so we exclude p2
			excluded.add(p2);
		    }
		    if (scr.getStatus() == 2) {
			// p2 contains p1, so we exclude p1
			excluded.add(p1);
		    }
		    if (scr.getStatus() == 3) {
			//p1 equals p2, so we exclude both
			excluded.add(p2);
			excluded.add(p1);
		    }

		    // common size greater than M
		    // create a new pattern based on the common size
		    Pattern newp = new Pattern();
		    newp.insertObjects(scr.getCommons());
		    for (Integer t : p1.getTimeSet()) {
			newp.insertTime(t);
		    }
		    for (Integer t : p2.getTimeSet()) {
			newp.insertTime(t);
		    }
		    result.add(newp);
		}
	    }
	    // for each old patterns, determine whether to further include p1 or
	    // p2
	    // this can prune many unnecessary patterns
	    for (Pattern p1 : v1) {
		if (!excluded.contains(p1)) {
		    if (TemporalVerification.isLGValidTemporal(p1.getTimeSet(),
			    L, G)) {
			result.add(p1);
		    }
		}
	    }

	    for (Pattern p2 : v2) {
		if (!excluded.contains(p2)) {
		    if (TemporalVerification.isLGValidTemporal(p2.getTimeSet(),
			     L, G)) {
			result.add(p2);
		    }
		}
	    }
	    return result;
	}
    };
    
    private Function2<ArrayList<Pattern>, ArrayList<Pattern>, ArrayList<Pattern>> FUNC2 =  new Function2<ArrayList<Pattern>, ArrayList<Pattern>, ArrayList<Pattern>>() {
	private static final long serialVersionUID = 6875935653511633616L;

	@Override
	public ArrayList<Pattern> call(
		ArrayList<Pattern> v1,
		ArrayList<Pattern> v2) throws Exception {
	    ArrayList<Pattern> result = new ArrayList<>();
	    HashSet<Pattern> excluded = new HashSet<Pattern>();
	    // TODO:: further pruning exists. For
	    // example, we can prune the
	    // patterns that has is far beyond gap G
	    for (Pattern p1 : v1) {
		for (Pattern p2 : v2) {
		    if (p1.getEarlyTS()
			    - p2.getLatestTS() > G) {
			continue;
		    }
		    if (p2.getEarlyTS()
			    - p1.getLatestTS() > G) {
			continue; // no need to consider
		    }
		    Set<Integer> s1 = p1.getObjectSet();
		    Set<Integer> s2 = p2.getObjectSet();
		    SetCompResult scr = SetOps
			    .setCompare(s1, s2);
		    // check common size
		    if (scr.getCommonsSize() < M) {
			// then the only pattern is p1
			// and p2
			continue;
		    }
		    if (scr.getStatus() == 1) {
			// p1 contains p2, so we exclude
			// p2
			excluded.add(p2);
		    }
		    if (scr.getStatus() == 2) {
			// p2 contains p1, so we exclude
			// p1
			excluded.add(p1);
		    }
		    if (scr.getStatus() == 3) {
			// p1 equals p2, so we exclude
			// both
			excluded.add(p2);
			excluded.add(p1);
		    }
		    // common size greater than M
		    // create a new pattern based on the
		    // common size
		    Pattern newp = new Pattern();
		    newp.insertObjects(scr.getCommons());
		    for (Integer t : p1.getTimeSet()) {
			newp.insertTime(t);
		    }
		    for (Integer t : p2.getTimeSet()) {
			newp.insertTime(t);
		    }
		    result.add(newp);
		}
	    }
	    
	    //prune out-of-ranged patterns
	    int p1_latest = -1, p1_earliest = Integer.MAX_VALUE;
	    int p2_latest = -1, p2_earliest = Integer.MAX_VALUE;
	    for(Pattern p1 : v1) {
		if(p1.getLatestTS() > p1_latest) {
		    p1_latest = p1.getLatestTS();
		} 
		if(p1.getEarlyTS() < p1_earliest) {
		    p1_earliest = p1.getEarlyTS();
		}
	    }
	    for(Pattern p2 : v2) {
		if(p2.getLatestTS() > p2_latest) {
		    p2_latest = p2.getLatestTS();
		} 
		if(p2.getEarlyTS() < p2_earliest) {
		    p2_earliest = p2.getEarlyTS();
		}
	    }
	    int earliest = p1_earliest > p2_earliest ?  p2_earliest : p1_earliest;
	    int latest = p1_latest > p2_latest ? p1_latest : p2_latest;
	    for(Pattern p1 : v1) {
		if(p1.getEarlyTS() - earliest > G
		&& latest -  p1.getLatestTS() > G) {
		    excluded.add(p1);
		}
	    }
	    for(Pattern p2 : v2) {
		if(p2.getEarlyTS() - earliest > G
		&& latest -  p2.getLatestTS() > G) {
		    excluded.add(p2);
		}
	    }
	    // for each old patterns, determine whether
	    // to further include p1 or
	    // p2
	    // this can prune many unnecessary patterns
	    for (Pattern p1 : v1) {
		if (!excluded.contains(p1)) {
		    // if
		    // (TemporalVerification.isLGValidTemporal(p1.getTimeSet(),
		    // L, G)) {
		    result.add(p1);
		    // }
		}
	    }
	    for (Pattern p2 : v2) {
		if (!excluded.contains(p2)) {
		    // if
		    // (TemporalVerification.isLGValidTemporal(p2.getTimeSet(),
		    // L, G)) {
		    result.add(p2);
		    // }
		}
	    }
	    return result;
	}
    };

    public static void main(String[] args) throws Exception {
	int k = 5;
	int l = 3;
	int g = 2;
	int m = 3;
	PatternReduceTest prt = new PatternReduceTest(m, l, g, k);
	ArrayList<Pattern> p1 = new ArrayList<Pattern>();
	Pattern p = new Pattern(new int[] { 1, 2, 3 }, new int[] { 1, 2, 3, 4 });
	p1.add(p);

	ArrayList<Pattern> p2 = new ArrayList<Pattern>();
	p = new Pattern(new int[] { 1, 2, 3 }, new int[] { 5, 6, 7, 8 });
	p2.add(p);
	p = new Pattern(new int[] { 4, 5, 6, 7 }, new int[] { 5, 6, 7 });
	p2.add(p);

	ArrayList<Pattern> p12R = prt.FUNC2.call(p1, p2);
	System.out.println("First Join");
	for (Pattern ppp : p12R) {
	    System.out.println(ppp);
	}

	ArrayList<Pattern> p3 = new ArrayList<>();
	p = new Pattern(new int[] { 1, 2, 3 ,4}, new int[] { 10, 12,13});
	p3.add(p);

	ArrayList<Pattern> p123R = prt.FUNC2.call(p12R, p3);

	System.out.println("Second Join");
	for (Pattern ppp : p123R) {
	    System.out.println(ppp);
	}

    }
}
