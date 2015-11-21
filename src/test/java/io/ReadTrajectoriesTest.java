package io;
import static io.ReadTrajectories.*;
import util.Visualizer;
import model.Trajectory;
public class ReadTrajectoriesTest {
    
    public static void main(String[] args) {
	Trajectory[] trajectories = readTrajectories("dataset/Geolife/dis_flated.dat");
	System.out.println(trajectories.length);
	Visualizer.setScale(116.286798, 116.393756,39.887266, 40.009345);
	System.out.println(trajectories[0].getBoundingBox()[0]);
	System.out.println(trajectories[0].getBoundingBox()[1]);
	System.out.println(trajectories[0].getBoundingBox()[2]);
	System.out.println(trajectories[0].getBoundingBox()[3]);
	System.out.println(trajectories[2].getBoundingBox()[0]);
	System.out.println(trajectories[2].getBoundingBox()[1]);
	System.out.println(trajectories[2].getBoundingBox()[2]);
	System.out.println(trajectories[2].getBoundingBox()[3]);
	Visualizer.drawTrajectory(trajectories[0]);
	Visualizer.drawTrajectory(trajectories[1]);
	Visualizer.drawTrajectory(trajectories[2]);
    }
}
