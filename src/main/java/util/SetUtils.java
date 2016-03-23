package util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import util.SetComp.Result;


public class SetUtils {

    /**
     * compare the two input sets, return the comparison result together with
     * the commonly intersected objects
     * 
     * @param s1
     * @param s2
     * @return
     */
    public static SetComp compareSets(Set<Integer> s1, Set<Integer> s2) {
	Set<Integer> commons = new HashSet<Integer>();
	int s1size = s1.size();
	int s2size = s2.size();
	// interchange s1 and s2
	if (s1size > s2size) {
	    Set<Integer> tmp = s1;
	    s1 = s2;
	    s2 = tmp;
	}
	for (int i : s1) {
	    if (s2.contains(i)) {
		commons.add(i);
	    }
	}
	int csize = commons.size();
	if (s1size > csize) {
	    if (s2size > csize) {
		// both are not fully inclusive of each other
		return new SetComp(Result.NONE, commons);
	    } else {
		// s1 is super set
		return new SetComp(Result.SUPER, commons);
	    }
	} else {
	    if (s2size > csize) {
		// s2 is the super set
		return new SetComp(Result.SUB, commons);
	    } else {
		// s1 and s2 are equal
		return new SetComp(Result.EQUAL, commons);
	    }
	}
    }

    public static void main(String[] args) {
	Set<Integer> s1 = new HashSet<>(Arrays.asList(1, 2, 3, 4));
	Set<Integer> s2 = new HashSet<>(Arrays.asList(7, 5, 6));
	System.out.println(SetUtils.compareSets(s1, s2));
    }
}
