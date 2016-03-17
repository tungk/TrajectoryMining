package kforward;

import org.apache.spark.api.java.function.Function2;

import model.SnapShot;

public class SnapshotCombinor
	implements Function2<SnapShot, SnapShot, SnapShot> {
    private static final long serialVersionUID = -3440630456333492120L;

    @Override
    public SnapShot call(SnapShot v1, SnapShot v2) throws Exception {
	v1.MergeWith(v2);
	return v1;
    }
}
