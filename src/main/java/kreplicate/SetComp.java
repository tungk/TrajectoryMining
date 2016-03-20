package kreplicate;

import java.io.Serializable;
import java.util.Set;


/**
 * store the result for set comparison
 * @author a0048267
 *
 */
public class SetComp implements Serializable{
    private static final long serialVersionUID = -163453000970292923L;
	public static enum Result{
		SUPER,
		EQUAL,
		SUB,
		NONE,
	}
	private Set<Integer> intersect;
	private Result type;
	public SetComp(Result r, Set<Integer> common) {
		type = r;
		intersect = common;
	}
	
	public Set<Integer> getIntersect() {
		return intersect; 
	}
	
	public Result getResult() {
		return type;
	}
	
	@Override
	public String toString() {
		return String.format("[%s,%s]", type, intersect);
	}
}
