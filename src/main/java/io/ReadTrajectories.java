package io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import model.TemporalPoint;
import model.Trajectory;

/**
 * reading Trajectories from datafile
 * a Trajectory is represented by a sequence of tuple
 * in file
 * In the program, it is represented by an array of ArrayList of TemporalPoint
 * This read is for reading at local machine
 * @author a0048267
 *
 */
public class ReadTrajectories {
    public static Trajectory[] readTrajectories(String inputPath) {
	FileReader fr;
	BufferedReader br;
	try {
	    fr = new FileReader(inputPath);
	    br = new BufferedReader(fr);
	    String line;
	    String[] par;
	    line = br.readLine();
	    int num_of_objects = Integer.parseInt(line);
	    Trajectory[] trajectories = new Trajectory[num_of_objects];
	    for(int i = 0; i < num_of_objects; i++) {
		trajectories[i] = new Trajectory();
	    }
	    while((line = br.readLine()) != null) {
		par = line.split("\t");
		int object = Integer.parseInt(par[0]);
		trajectories[object].add(new TemporalPoint(
			Double.parseDouble(par[1]),
			Double.parseDouble(par[2]),
			Integer.parseInt(par[3])));
	    }
	    br.close();
	    return trajectories;
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return null;
    }
}
