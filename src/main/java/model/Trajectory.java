package model;

import java.util.ArrayList;

/**
 * A Trajectory is an encapsulated ArrayList of temporal point, with the
 * constraint that the temporal points are in sorted order We also require that
 * a trajectory is append only, and the OID is its key
 * 
 * @author a0048267
 * 
 */
public class Trajectory extends ArrayList<TemporalPoint> {
    private static final long serialVersionUID = 6700721732576703598L;
    private static int IDS = 0;
    private int oid;
    private int size;
    private double x1, y1, x2, y2; // these are the minimum bounding box of this
				   // trajectory

    /**
     * create an empty trajectory
     */
    public Trajectory() {
	oid = IDS++;
	size = 0;
	x1 = 99999;
	y1 = 99999;
	x2 = 0;
	y2 = 0;
    }

    /**
     * insert a TemporalPoint to current trajectory out-of-order insertion is
     * rejected
     * 
     * @param tp
     * @return
     */
    public boolean insertPoint(TemporalPoint tp) {
	if (size == 0) {
	    add(tp);
	    size++;
	    return true;
	} else if (tp.getTime() > this.get(size - 1).getTime()) {
	    add(tp);
	    size++;
	    return true;
	} else {
	    return false;
	}
    }
    
    @Override
    public boolean add(TemporalPoint tp) {
	double tla = tp.getLat();
	double tlg = tp.getLont();
	if(tla > x2) {
	    x2 = tla;
	} else if(tla < x1) {
	    x1 = tla;
	}
	if(tlg > y2) {
	    y2 = tlg;
	} else if (tlg < y1) {
	    y1 = tlg;
	}
	super.add(tp);
	return true;
    }
    
    public double[] getBoundingBox() {
	return new double[]{x1,y1,x2,y2};
    }

    public int getID() {
	return oid;
    }

    /**
     * Count the current trajectory length
     * 
     * @return
     */
    public int getLength() {
	return size;
    }

    @Override
    public String toString() {
	return oid + "(" + size + "):" + super.toString();
    }
}
