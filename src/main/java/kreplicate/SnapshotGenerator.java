package kreplicate;

import org.apache.spark.api.java.function.PairFunction;

import model.Point;
import model.SnapShot;
import scala.Tuple2;

public class SnapshotGenerator
	implements PairFunction<String, Integer, SnapShot> {

    private static final long serialVersionUID = 5397774089383139053L;

    @Override
    public Tuple2<Integer, SnapShot> call(String t) throws Exception {
	String[] splits = t.split("\t");
	int obj_key = Integer.parseInt(splits[0]);
	int ts = Integer.parseInt(splits[3]);
	Point p = new Point(Double.parseDouble(splits[1]),
		Double.parseDouble(splits[2]));
	SnapShot sp = new SnapShot(ts);
	sp.addObject(obj_key, p);
	return new Tuple2<Integer, SnapShot>(ts, sp);
    }
}