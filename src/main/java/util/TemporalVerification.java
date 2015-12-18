package util;

import java.io.Serializable;
import java.util.List;

public class TemporalVerification implements Serializable {
    private static final long serialVersionUID = -4470937618770383806L;

    public static boolean isValidTemporal(List<Integer> temporal, int K, int L,
	    int G) {
	if (temporal.size() < K) {
	    return false;
	}
	return isLGValidTemporal(temporal, L, G);
    }

    /**
     * only needs to test whether this one is valid or not. do not need to
     * decompose or extract
     * @param temporal
     * @param L
     * @param G
     * @return
     */
    public static boolean isLGValidTemporal(List<Integer> temporal, int L, int G) {
	// for every consecutive part, check whether it is l-consecutive
	int consecutive = 1;
	for (int i = 1; i < temporal.size(); i++) {
	    int delta = temporal.get(i) - temporal.get(i - 1);
	    if (delta == 1) {
		consecutive++;
		continue;
	    } else if (temporal.get(i) - temporal.get(i - 1) <= G) {
		if (consecutive >= L) {
		    consecutive = 1;
		    continue;
		} else {
		    return false;
		}
	    } else {
		return false;
	    }
	}
	return true;
    }
}
