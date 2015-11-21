package model;

import java.io.Serializable;

/**
 * point is a pair of coordinate in a spatio domain
 * in our application, a point is immutable
 * @author a0048267
 */
public class Point implements Serializable {
    private static final long serialVersionUID = 101161731896673554L;
    private final double lat;
    private final double lont;
    private final int hashcode;
    
    public Point(double lat, double lont) {
	this.lat = lat;
	this.lont = lont;
	final int prime = 31;
	int result = 1;
	long temp;
	temp = Double.doubleToLongBits(lat);
	result = prime * result + (int) (temp ^ (temp >>> 32));
	temp = Double.doubleToLongBits(lont);
	result = prime * result + (int) (temp ^ (temp >>> 32));
	hashcode = result;
    }
    
    public double getLat() {
        return lat;
    }
    public double getLont() {
        return lont;
    }
    
    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if(obj instanceof Point) {
	    if(Math.abs(((Point) obj).getLat() - lat) < 0.0000001) {
		if(Math.abs(((Point) obj).getLont() - lont) < 0.0000001) {
		    return true;
		}
	    }
	}
	return false;
    }
    @Override
    public int hashCode() {
	return hashcode;
    }
}
