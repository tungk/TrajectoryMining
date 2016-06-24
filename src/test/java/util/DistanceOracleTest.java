package util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.commons.lang3.tuple.Pair;

import model.Point;

public class DistanceOracleTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
	// Point p1 = new Point(1.377730, 103.774460);
	// Point p2 = new Point(1.379230, 103.768770);
	// System.out.println(DistanceOracle.compEarthDistance(p1, p2));
	//
	// p1 = new Point(5.76, 0.18);
	// p2 = new Point(5.86, 0.33);
	// System.out.println(DistanceOracle.compEuclidianDistance(p1, p2));
	int l = 3;
	int k = 6;
	int g = 2;
	ArrayList<Integer> temporals = new ArrayList<Integer>(Arrays.asList(1,
		2, 3, 6, 7, 8, 9, 15, 16, 17, 25, 30, 31, 32, 33));
	LinkedList<ArrayList<Integer>> cons = new LinkedList<>();
	ArrayList<Integer> con = new ArrayList<>();
	for (int i = 1; i < temporals.size(); i++) {
	    int diff = temporals.get(i) - temporals.get(i - 1);
	    con.add(temporals.get(i - 1));
	    if (diff != 1) {
		if(con.size() >= l) {
		    cons.add(con);
		}
		con = new ArrayList<>();
	    }
	}
	cons.add(con);
	System.out.println(cons);
	
	int[] gcps = new int[5];
	for(int G = 1; G <10; G += 2) {
	    int accum = cons.get(0).size();
	    for(int c = 1; c < cons.size(); c++) {
		if(cons.get(c).get(0)- cons.get(c-1).get(cons.get(c-1).size() -1)
			 >G) {
		    if(accum >= k) {
			gcps[G/2]++;
			break;
		    } else {
			accum = cons.get(c).size();
		    }
		} else {
		    accum += cons.get(c).size();
		}
	    }
	    if(accum >= k) {
		gcps[G/2]++;
	    }
	}
	System.out.println(gcps[0]+"\t"+gcps[1]+"\t"+gcps[2]+"\t"+gcps[3]+"\t"+gcps[4]);
    }

}
