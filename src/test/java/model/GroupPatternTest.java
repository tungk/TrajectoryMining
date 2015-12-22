package model;

public class GroupPatternTest {
    public static void main(String[] args) {
	Pattern p1 = new Pattern(new int[]{11,12,13,14,15,16},new int[]{1,2,3,4});
	System.out.println(p1);
	GroupPatterns gp = new GroupPatterns();
	gp.insert(p1);
	System.out.println(gp);
    }
}
