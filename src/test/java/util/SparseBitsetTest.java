package util;

import com.zaxxer.sparsebits.SparseBitSet;

public class SparseBitsetTest {
    public static void main(String[] args) {
	SparseBitSet sbs = new SparseBitSet();
	System.out.println(sbs.statistics());
	
	sbs.set(1);
	System.out.println(sbs.statistics());
    }
}
