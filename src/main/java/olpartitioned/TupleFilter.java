package olpartitioned;

import org.apache.spark.api.java.function.Function;

public class TupleFilter implements Function<String, Boolean> {
    private static final long serialVersionUID = -3228053096445538351L;
    @Override
    public Boolean call(String v1) throws Exception {
	if (v1.isEmpty() || v1.charAt(0) == '#') {
	    return false;
	} else {
	    return true;
	}
    }
}
