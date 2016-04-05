package util;

import com.zaxxer.sparsebits.SparseBitSet;

public class SparseBitSetUtil {
    /**
     * given a SparseBitSet, print it in the full format
     * @param sbs
     * @return
     */
    public static String printSparseBitSet(SparseBitSet sbs) {
	StringBuilder sb = new StringBuilder();
//	StringBuffer sb = new StringBuffer();
	for( int i = sbs.nextSetBit(0); i >= 0; i = sbs.nextSetBit(i+1) ) {
	           // operate on index i here
	    sb.append(i).append(',');
	}
	if(sb.length() > 0) {
	    sb.deleteCharAt(sb.length() -1);
	}
	return sb.toString();
    }
}
