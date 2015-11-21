package util;

import model.TemporalPoint;
import model.Trajectory;

public class VisualizerTest {
    public static void main(String[] args) {
	Trajectory tr = new Trajectory();
	tr.insertPoint(new TemporalPoint(1.2562, 2.47589, 3));
	tr.insertPoint(new TemporalPoint(2.2562, 3.47589, 4));
	tr.insertPoint(new TemporalPoint(1.2562, 7.47589, 5));
	tr.insertPoint(new TemporalPoint(3.2562, 7.47589, 6));
	tr.insertPoint(new TemporalPoint(7.2562, 1.47589, 7));
	tr.insertPoint(new TemporalPoint(8.2562, 2.47589, 8));
	Visualizer.drawTrajectory(tr);
	
	Trajectory tr2 = new Trajectory();
	tr2.insertPoint(new TemporalPoint(12.47589,10.2562,  3));
	tr2.insertPoint(new TemporalPoint(13.47589,11.2562,  4));
	tr2.insertPoint(new TemporalPoint(17.47589,10.2562,  5));
	tr2.insertPoint(new TemporalPoint(17.47589,13.2562,  6));
	tr2.insertPoint(new TemporalPoint(11.47589,17.2562,  7));
	tr2.insertPoint(new TemporalPoint(12.47589,28.2562,  8));
	tr2.insertPoint(new TemporalPoint(12.47589,18.2562,  9));
	Visualizer.drawTrajectory(tr2);
	
	Trajectory tr3 = new Trajectory();
	tr3.insertPoint(new TemporalPoint(2.47589,30.2562,  3));
	tr3.insertPoint(new TemporalPoint(3.47589,31.2562,  4));
	tr3.insertPoint(new TemporalPoint(7.47589,30.2562,  5));
	tr3.insertPoint(new TemporalPoint(7.07589,33.2562,  6));
	tr3.insertPoint(new TemporalPoint(1.27589,37.2562,  7));
	tr3.insertPoint(new TemporalPoint(2.17589,38.2562,  8));
	tr3.insertPoint(new TemporalPoint(2.37589,38.2562,  9));
	Visualizer.drawTrajectory(tr3);
    }
}
