package olpartitioned;

import model.SnapShot;

import org.apache.spark.api.java.function.Function2;

public class SnapshotCombinator implements
	Function2<SnapShot, SnapShot, SnapShot> {
    private static final long serialVersionUID = -46541894932044237L;

    @Override
    public SnapShot call(SnapShot v1, SnapShot v2) throws Exception {
	v1.MergeWith(v2);
	return v1;
    }

}
