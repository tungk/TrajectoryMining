package model;

import util.DistanceOracle;

public class PointTest {
    public static void main(String[] args) {
	Point p1 = new Point(117.23514235, 31.1265929);
	Point p2 = new Point(117.23514578, 31.1275929);
	System.out.println(p1);
	System.out.println(p2);
	System.out.println(DistanceOracle.dist(p1, p2));
    }
}	
