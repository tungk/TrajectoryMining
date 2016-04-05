package util;

import com.zaxxer.sparsebits.SparseBitSet;

public class SparseBitsetTest {
    public static void main(String[] args) {
	SparseBitSet sbs = new SparseBitSet();
	sbs.set(1);
	sbs.set(3);
	sbs.set(5);
	sbs.set(7);
	
	SparseBitSet sbs2 = new SparseBitSet();
	sbs2.set(2);
	sbs2.set(4);
	sbs2.set(6);
	sbs2.set(7);
	sbs2.set(8);
	sbs2.set(4700);
	
	
	SparseBitSet sbs3 = new SparseBitSet();
	sbs3.set(3001);
	sbs3.set(1);
	sbs3.set(3002);
	sbs3.set(300);
	
	sbs.or(sbs3);
	System.out.println(SparseBitSetUtil.printSparseBitSet(sbs));
	sbs.or(sbs2);
	System.out.println(SparseBitSetUtil.printSparseBitSet(sbs));
	
//	System.out.println(sbs);

    }
}
