package util;

import static conf.Constants.*;
import model.Point;

public class DistanceOracle {
    /**
     * given the two point's coordinate in latitude and longitude, we wish to
     * find the distance in meters. Here we do not need to use ``haversine'' 
     * formula
     * since the points are near, thus can be viewed as a triangle.
     * @param p1 first point
     * @param p2 second point
     * @return distance in meters
     */
    public static double dist(Point p1, Point p2) {
	double lat1 = p1.getLat();
	double lat2 = p2.getLat();
	double lont1 = p1.getLont();
	double lont2 = p2.getLont();
	// convert to meter
	double latDistance = Math.toRadians(lat2-lat1) * EARTH_RADIUS;
	double lonDistance = Math.toRadians(lont2-lont1) * EARTH_RADIUS;
	return Math.sqrt(Math.pow(latDistance, 2) + Math.pow(lonDistance, 2)) * 1000; 
    }
}