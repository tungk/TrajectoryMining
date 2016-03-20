package kreplicate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

/**
 * A self-adjusted pattern, which always update
 * its self in a valid temporal domain.
 * @author a0048267
 *
 */
public class Pattern {
	private int M, L, K, G;
	private HashSet<Integer> objects;
	private ArrayList<Integer> tstamps;
	//design some indicators on temporal aspects;
	private boolean ob_valid;
	private int last_con_start; //record the where the last consectuive part starts;
	
	public Pattern(int m, int l, int k, int g) {
		objects = new HashSet<>();
		tstamps = new ArrayList<>();
		ob_valid = true;
		M = m; L = l; K = k; G = g;
		last_con_start = -1; //the value is unsetted;
	}
	
	/**
	 * only used at initialization step
	 * @param objects
	 */
	public void addObjects(Collection<Integer> objects) {
		this.objects.addAll(objects);
		if(this.objects.size() < M) {
			ob_valid = false;
		}
	}
	
	/**
	 * this returns a reference of the object set
	 * of current pattern;
	 * @return
	 */
	public HashSet<Integer> getObjects() {
		return objects;
	}
	
	public ArrayList<Integer> getTstamps() {
		return this.tstamps;
	}
	
	
	
	/**
	 * checks whether adding growing current temporal
	 * dim by adding ts is valid or not
	 * if returns true, the ts is added to the pattern, the resulting
	 * pattern would be still valid
	 * if false, the ts is added, but the temporal validity is set to be false
	 * @param ts
	 * @return
	 */
	public boolean growTemporal(int ts) {
		if(tstamps.isEmpty()) {
			tstamps.add(ts);
			last_con_start = 0;
			return true;
		} else {
			int next_index = tstamps.size();
			int prev = tstamps.get(next_index - 1) ;
			if(ts - prev > G) {
				return false; //do not add ts in, do nothing to current status
			} else if(ts - prev != 1) {
				//is able to add in, but we need to check the consecutiveness of previous part
				int prev_consecutive = next_index - last_con_start;
				if(prev_consecutive >= L) {
					tstamps.add(ts);
					last_con_start = next_index;
					return true;
				} else {
					//the prev consecutive is not long enough like 1,2,3,5, we need to remove 5 to test gap again
					ArrayList<Integer> newstamps = new ArrayList<>();
					for(int i = 0; i < last_con_start; i++) {
						newstamps.add(tstamps.get(i));
					}
					if(newstamps.isEmpty() || ts - newstamps.get(newstamps.size() - 1) <=G) {
						//last_con_start do not change
						newstamps.add(ts);
						tstamps = newstamps;
						return true;
					} else {
						return false;
					}
				}
			} else {
				tstamps.add(ts);
				return true;
			}
		}
	}
	
	/**
	 * reset temporal domain to initial state;
	 */
	public void clearTemporals() {
		tstamps.clear();
		last_con_start = -1;
	}

	@Override
	public String toString() {
		return String.format("<%s x %s>", objects,tstamps);
	}

	public void addTemporals(ArrayList<Integer> tstamps2) {
		for(int i : tstamps2) {
			this.growTemporal(i);
		}
	}
	
	public boolean checkParitialValidity() {
		return ob_valid; // the temporal is always partially valid
	}
	
	public boolean checkFullValidity() {
		boolean tp_valid = true;
		int last_ts = getLatestTS();
		if(last_ts - last_con_start + 1 < L) {
			tp_valid = false;
		}
		if(tstamps.size() < K) {
			tp_valid = false;
		}
		return ob_valid && tp_valid;
	}
	
	public static void main(String[] args) {
		int K = 6;
		int M = 1;
		int G = 4;
		int L = 2;
		ArrayList<Integer> objects = new ArrayList<Integer>(Arrays.asList(1,2,3));
//		ArrayList<Integer> temporls = new ArrayList<Integer>(Arrays.asList(0,1,2,3,4));
		Pattern p = new Pattern(M, L, K, G);
		p.addObjects(objects);
//		p.addTemporals(temporls);
		int[] temporals = new int[]{0, 2, 3, 4, 7, 8, 9, 11, 13, 14, 15};
		for(int i : temporals) {
			System.out.print(p.growTemporal(i)+"\t");
			System.out.println(p);
		}
	}

	public int getLatestTS() {
		if(tstamps.size() == 0) {
			return -1;
		}
		return tstamps.get(tstamps.size() - 1);
	}

	public int getEarlistTS() {
	    if(tstamps.size() == 0) {
		return -1;
	    } else {
		return tstamps.get(0);
	    }
	}
}
