package apriori;

import java.util.Collection;

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.function.Function;

public class DuplicateClusterFilter implements
	Function<IntSet, Boolean> {
    private static final long serialVersionUID = 8171848799948095816L;
    private final Collection<IntSet> grounds;

    public DuplicateClusterFilter(Collection<IntSet> ground) {
	grounds = ground;
    }
    
    @Override
    public Boolean call(IntSet v1) throws Exception {
	System.out.println("Ground:"+grounds+"\nValue:"+v1);
	// remove duplicate intset
	for (IntSet ground : grounds) {
	    if(ground.containsAll(v1) && ground.size() > v1.size()) {
		return false;
	    }
	}
	return true;
    }
}
