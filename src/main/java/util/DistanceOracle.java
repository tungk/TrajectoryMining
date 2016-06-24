package util;

import static conf.Constants.*;
import model.Point;

public class DistanceOracle {
    /**
     * given the two point's coordinate in latitude and longitude, we wish to
     * find the distance in meters. Here we do not need to use ``haversine'' 
     * formula because the points are near, and they can be viewed as a triangle.
     * @param p1 first point
     * @param p2 second point
     * @return distance in meters
     */
    public static double compEarthDistance(Point p1, Point p2) {
	if(p1 == null || p2 == null) {
	    return Double.MAX_VALUE;
	}
	double lat1 = p1.getLat();
	double phi1 = Math.toRadians(lat1);
	double lat2 = p2.getLat();
	double phi2 = Math.toRadians(lat2);
	double delta_phi = Math.toRadians(lat2-lat1);
	double lont1 = p1.getLont();
	double lont2 = p2.getLont();
	double delta_lambda = Math.toRadians(lont2-lont1);
	
	double a = Math.pow(Math.sin(delta_phi)/2,2) +
		   Math.cos(phi1)*Math.cos(phi2)* Math.pow(Math.sin(delta_lambda/2),2);
	double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
	return c * EARTH_RADIUS * 1000;
    }
    
    public static double compEuclidianDistance(Point p1, Point p2) {
	if(p1 == null || p2 == null) {
	    return Double.MAX_VALUE;
	}
	double x1 = p1.getLat(), y1 = p1.getLont();
	double x2 = p2.getLat(), y2 = p2.getLont();
	double xdiff = x2-x1;
	double ydiff = y2-y1;
	return Math.sqrt(xdiff*xdiff + ydiff*ydiff) * 10; //to meters;
    }
}