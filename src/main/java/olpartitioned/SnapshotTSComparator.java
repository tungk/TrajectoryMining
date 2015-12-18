package olpartitioned;

import common.SerializableComparator;
import model.SnapShot;
import scala.Tuple2;

public class SnapshotTSComparator {
    public static SerializableComparator<Tuple2<Integer, SnapShot>> minComp = new SerializableComparator<Tuple2<Integer, SnapShot>>() {
	private static final long serialVersionUID = -3670967892723637957L;
	@Override
	public int compare(Tuple2<Integer, SnapShot> o1,
		Tuple2<Integer, SnapShot> o2) {
	    return o1._1 - o2._1;
	}
    };
    
    public static SerializableComparator<Tuple2<Integer, SnapShot>> maxComp = new SerializableComparator<Tuple2<Integer, SnapShot>>() {
	private static final long serialVersionUID = 2934457668404660612L;
	@Override
   	public int compare(Tuple2<Integer, SnapShot> o1,
   		Tuple2<Integer, SnapShot> o2) {
   	    return o2._1 - o1._1;
   	}
       };
}
