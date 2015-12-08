package util;

import java.util.Set;

public class SetOps {
    /**
     * 
     * @param s1
     * @param s2
     */
    public static SetCompResult setCompare(Set<Integer> s1, Set<Integer> s2) {
	SetCompResult res = new SetCompResult();
	boolean s1_contain_s2 = true;
	boolean s2_contain_s1 = true;
	for(Integer i : s1) {
	    if(!s2.contains(i)) {
		s2_contain_s1 = false;
	    } else {
		res.addCommons(i);
	    }
	}
	for(Integer i: s2) {
	    if(!s1.contains(i)) {
		s1_contain_s2 = false;
	    }
	}
	if(s1_contain_s2) {
	    res.setStatus(1);
	}
	if(s2_contain_s1) {
	    res.setStatus(2);
	}
	if(s1_contain_s2 && s2_contain_s1) {
	    res.setStatus(3);
	}
	return res;
	
    }
}
