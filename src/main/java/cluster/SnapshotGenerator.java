package cluster;

import org.apache.spark.api.java.function.PairFunction;

import model.Point;
import model.SnapShot;
import scala.Tuple2;

public class SnapshotGenerator
	implements PairFunction<String, Integer, SnapShot> {

    private static final long serialVersionUID = 5397774089383139053L;

    private String[] splits;
    private int obj_key;
    private int ts;
    private Point p;
    
    @Override
    public Tuple2<Integer, SnapShot> call(String t) throws Exception {
	splits = t.split("\t");
	obj_key = Integer.parseInt(splits[0]);
	ts = Integer.parseInt(splits[3]);
	p = new Point(Double.parseDouble(splits[1]),
		Double.parseDouble(splits[2])); //we need to new a point for every tuple, 
						//the reference of the object will only be used once
	SnapShot sp = new SnapShot(ts);
	sp.addObject(obj_key, p);
	return new Tuple2<Integer, SnapShot>(ts, sp);
    }
}