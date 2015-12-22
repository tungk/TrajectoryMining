package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import util.SetCompResult;
import util.SetOps;

public class GroupPatterns implements Serializable {
    private HashSet<Pattern> patterns;
    private int start, end; // the [start,end] range the patterns inside the
			    // group

    /**
     * create a new set of patterns
     */
    public GroupPatterns() {
	patterns = new HashSet<>();
	start = Integer.MAX_VALUE;
	end = Integer.MIN_VALUE;
    }

    public GroupPatterns(Pattern p) {
	patterns = new HashSet<>();
	patterns.add(p);
	start = p.getEarlyTS();
	end = p.getLatestTS();
    }

    /**
     * checks whether the given pattern is in the group
     * 
     * @param p
     * @return
     */
    public boolean contains(Pattern p) {
	return patterns.contains(p);
    }

    public void insert(Pattern p) {
	patterns.add(p);
	if (start > p.getEarlyTS()) {
	    start = p.getEarlyTS();
	}
	if (end < p.getLatestTS()) {
	    end = p.getLatestTS();
	}
    }

    public int size() {
	return patterns.size();
    }

    public int getStart() {
	return start;
    }

    public int getEnd() {
	return end;
    }

    public Iterable<Pattern> getPatterns() {
	return patterns;
    }

    /**
     * Join two group patterns to form a new group pattern During join, we
     * remove the group patterns
     * 
     * @param gp1
     * @param gp2
     * @return
     */
    public static GroupPatterns joinGPs(GroupPatterns gp1, GroupPatterns gp2,
	    int G, int M, int L) {
	GroupPatterns result = new GroupPatterns();
	HashSet<Pattern> excluded = new HashSet<Pattern>();
	// TODO:: further pruning exists. For
	// example, we can prune the
	// patterns that has is far beyond gap G
	for (Pattern p1 : gp1.getPatterns()) {
	    for (Pattern p2 : gp2.getPatterns()) {
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
		result.insert(newp);
	    }
	}

	// prune out-of-ranged patterns
	int p1_latest = -1, p1_earliest = Integer.MAX_VALUE;
	int p2_latest = -1, p2_earliest = Integer.MAX_VALUE;
	for (Pattern p1 : gp1.getPatterns()) {
	    if (p1.getLatestTS() > p1_latest) {
		p1_latest = p1.getLatestTS();
	    }
	    if (p1.getEarlyTS() < p1_earliest) {
		p1_earliest = p1.getEarlyTS();
	    }
	}
	for (Pattern p2 : gp2.getPatterns()) {
	    if (p2.getLatestTS() > p2_latest) {
		p2_latest = p2.getLatestTS();
	    }
	    if (p2.getEarlyTS() < p2_earliest) {
		p2_earliest = p2.getEarlyTS();
	    }
	}
	int earliest = p1_earliest > p2_earliest ? p2_earliest : p1_earliest;
	int latest = p1_latest > p2_latest ? p1_latest : p2_latest;
	for (Pattern p1 : gp1.getPatterns()) {
	    if (p1.getEarlyTS() - earliest > G && latest - p1.getLatestTS() > G) {
		excluded.add(p1);
	    }
	    if (p1.getEarlyTS() - earliest != 0
		    || latest - p1.getLatestTS() != 0) {
		if (p1.getTimeSize() < L) {
		    excluded.add(p1);
		}
	    }
	}
	for (Pattern p2 : gp2.getPatterns()) {
	    if (p2.getEarlyTS() - earliest > G && latest - p2.getLatestTS() > G) {
		excluded.add(p2);
	    }
	    if (p2.getEarlyTS() - earliest != 0
		    || latest - p2.getLatestTS() != 0) {
		if (p2.getTimeSize() < L) {
		    excluded.add(p2);
		}
	    }
	}
	// for each old patterns, determine whether
	// to further include p1 or
	// p2
	// this can prune many unnecessary patterns
	for (Pattern p1 : gp1.getPatterns()) {
	    if (p1.getObjectSize() >= M) {
		if (!excluded.contains(p1)) {
		    result.insert(p1);
		}
	    }
	}
	for (Pattern p2 : gp2.getPatterns()) {
	    if (p2.getObjectSize() >= M) {
		if (!excluded.contains(p2)) {
		    result.insert(p2);
		}
	    }
	}
	return result;
    }
}
