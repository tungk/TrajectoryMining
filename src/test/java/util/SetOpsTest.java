package util;

import java.util.HashSet;
import java.util.Set;

public class SetOpsTest {
    public static void main(String[] args) {
	Set<Integer> set1 = new HashSet<>();
	set1.add(1);set1.add(3);set1.add(5);set1.add(7);set1.add(10);set1.add(11);
	Set<Integer> set2 = new HashSet<>();
	set2.add(1);set2.add(3);set2.add(5);set2.add(11);
	System.out.println(SetOps.setCompare(set1, set2));
    }
}
