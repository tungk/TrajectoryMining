package model;

public class TrajectoryTest {
    public static void main(String[] args) {
	Trajectory tr = new Trajectory();
	System.out.println(tr);
	tr.insertPoint(new TemporalPoint(23.2562, 33.47589, 3));
	System.out.println(tr);
	tr.insertPoint(new TemporalPoint(23.2562, 33.47589, 4));
	System.out.println(tr);
	
	Trajectory tr2 = new Trajectory();
	System.out.println(tr2);
	tr2.insertPoint(new TemporalPoint(23.2562, 33.47589, 3));
	System.out.println(tr2);
	tr2.insertPoint(new TemporalPoint(23.2562, 33.47589, 4));
	System.out.println(tr2);
    }
}
