package util;

import java.awt.Color;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

import model.Cluster;
import model.Point;
import model.SnapShot;

import edu.princeton.cs.introcs.StdDraw;

public class SnapshotVisualizer {
    public static final double delta = 0.3;
    public static final Color[] colors = new Color[] { StdDraw.BLUE,
	    StdDraw.GRAY, StdDraw.GREEN, StdDraw.BOOK_BLUE, StdDraw.CYAN,
	    StdDraw.RED, StdDraw.PINK, StdDraw.MAGENTA, StdDraw.DARK_GRAY,
	    StdDraw.BOOK_LIGHT_BLUE, StdDraw.BOOK_RED };

    public static Random r = new Random();

    public static void drawSnapShot(SnapShot sp) {
	StdDraw.setScale(0, 1);
	StdDraw.setPenRadius(0.01);
	Set<Integer> objs = sp.getObjects();
	for (int obj : objs) {
	    StdDraw.setPenColor(colors[r.nextInt(colors.length)]);
	    Point p = sp.getPoint(obj);
	    StdDraw.point(p.getLat(), p.getLont());
	}
    }

    public static void drawClusters(ArrayList<Cluster> cluster,
	    SnapShot sp) {
	StdDraw.setScale(0, 1);
	StdDraw.setPenRadius(0.01);
	for (Cluster c : cluster) {
	    StdDraw.setPenColor(colors[r.nextInt(colors.length)]);
	    for (int obj : c.getObjects()) {
		Point p = sp.getPoint(obj);
		StdDraw.point(p.getLat(), p.getLont());
	    }
	}
    }
}
