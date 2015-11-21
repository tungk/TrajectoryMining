package util;

import java.awt.Color;
import java.util.Random;

import edu.princeton.cs.introcs.StdDraw;

import model.Trajectory;

/**
 * this class is a fun class which can visualize the input trajectories. Notice
 * that I didn't test those very long trajectories, thus be careful on the
 * drawing.
 * 
 * All methods are intended to be static, thus no context is required to be
 * maintained.
 * 
 * @author a0048267
 * 
 */
public class Visualizer {
    public static final double delta = 0.3;
    public static final Color[] colors = new Color[] { StdDraw.BLUE, StdDraw.GRAY,
	    StdDraw.GREEN, StdDraw.BOOK_BLUE, StdDraw.CYAN, StdDraw.RED,
	    StdDraw.PINK, StdDraw.MAGENTA, StdDraw.DARK_GRAY, 
	    StdDraw.BOOK_LIGHT_BLUE, StdDraw.BOOK_RED};
   
    public static Random r = new Random();
    
    public static void setScale(double xmin, double xmax, double ymin, double ymax) {
	StdDraw.setXscale(xmin, xmax);
	StdDraw.setYscale(ymin, ymax);
    }

    public static void drawTrajectory(Trajectory tr) {
	Color point = colors[r.nextInt(colors.length)];
	Color line = colors[r.nextInt(colors.length)];
	// draw out every points
	for (int i = 0; i < tr.size() - 1; i++) {
	    StdDraw.setPenRadius(0.02);
	    StdDraw.setPenColor(point);
	    if (i == 0) {
		StdDraw.point(tr.get(i).getLont(), tr.get(i).getLat());
		StdDraw.setPenColor(StdDraw.BLACK);
		StdDraw.text(tr.get(i).getLont(), tr.get(i).getLat() + delta,
			"<" + i + ">");
	    }
	    StdDraw.setPenColor(point);
	    StdDraw.point(tr.get(i + 1).getLont(), tr.get(i + 1).getLat());
	    StdDraw.setPenColor(StdDraw.BLACK);
	    StdDraw.text(tr.get(i + 1).getLont(), tr.get(i + 1).getLat()
		    + delta, "<" + (i + 1) + ">");

	    StdDraw.setPenColor(line);
	    StdDraw.setPenRadius(0.005);
	    StdDraw.line(tr.get(i).getLont(), tr.get(i).getLat(), tr.get(i + 1)
		    .getLont(), tr.get(i + 1).getLat());
	}
    }
}
