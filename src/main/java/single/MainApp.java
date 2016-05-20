package single;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;

import model.SnapShot;
import model.TemporalPoint;
import model.Trajectory;

/**
 */
public class MainApp {

    public static void main(String[] args) throws Exception {
	Class<?>[] classes = new Class[] { ConvoyPattern.class,
		FlockPattern.class, GroupPattern.class, SwarmPattern.class,
		PlatoonPattern.class };
	if (args.length < 8) {
	    printHelper();
	    System.exit(-1);
	} else {
	    int e = Integer.parseInt(args[0]);
	    int p = Integer.parseInt(args[1]);
	    int m = Integer.parseInt(args[2]);
	    int k = Integer.parseInt(args[3]);
	    int l = Integer.parseInt(args[4]);
	    int n = Integer.parseInt(args[5]);
	    int pattern_class = Integer.parseInt(args[6]);
	    String file_name = args[7];
	    PatternMiner pm = (PatternMiner) classes[pattern_class]
		    .newInstance();

	    pm.loadParameters(e, p, m, k, l);
	    ArrayList<Trajectory> trajs = loadData(file_name);
	    ArrayList<SnapShot> snapshots = transformSnap(trajs, n);
	    // snapshots are then feed to each pattern miner;
	    pm.loadData(snapshots);
	    pm.patternGen();
	    pm.printStats();
	}
    }

    private static ArrayList<SnapShot> transformSnap(
	    ArrayList<Trajectory> trajs, int maximum) {
	TreeMap<Integer, SnapShot> ts_shot_map = new TreeMap<>();
	for (Trajectory traj : trajs) {
	    int oid = traj.getID();
	    for (TemporalPoint tp : traj) {
		int t = tp.getTime();
		if (!ts_shot_map.containsKey(t)) {
		    ts_shot_map.put(t, new SnapShot(t));
		}
		ts_shot_map.get(t).addObject(oid, tp);
	    }
	}
	ArrayList<SnapShot> result = new ArrayList<>();
	if (maximum == -1) {
	    while (!ts_shot_map.isEmpty()) {
		int key = ts_shot_map.firstKey();
		result.add(ts_shot_map.get(key));
		ts_shot_map.remove(key);
	    }
	} else {
	    for (int i = 0; i < maximum; i++) {
		int key = ts_shot_map.firstKey();
		result.add(ts_shot_map.get(key));
		ts_shot_map.remove(key);
	    }
	}
	return result;
    }

    private static ArrayList<Trajectory> loadData(String file_name)
	    throws IOException {
	FileReader fr = new FileReader(file_name);
	BufferedReader br = new BufferedReader(fr);
	String line;
	line = br.readLine();
	ArrayList<Trajectory> trs = new ArrayList<>();
	Trajectory tr = new Trajectory();
	// int objects = Integer.parseInt(line);
	while ((line = br.readLine()) != null) {
	    String[] parts = line.split("\t");
	    int id = Integer.parseInt(parts[0]);
	    double posx = Double.parseDouble(parts[1]);
	    double posy = Double.parseDouble(parts[2]);
	    int t = Integer.parseInt(parts[3]);
	    if (id != tr.getID()) {
		trs.add(tr);
		tr = new Trajectory();
	    }
	    assert tr.getID() == id;
	    tr.insertPoint(new TemporalPoint(posx, posy, t));
	}
	if (!tr.isEmpty()) {
	    trs.add(tr);
	}
	br.close();
	return trs;
    }

    private static void printHelper() {
	System.out
		.println("[Usage]: java -cp TrajectoryMining-0.0.1-SNAPSHOT-jar-with-depedencies.jar single.MainApp E P M K L ClassIndicator Input");
    }
}
