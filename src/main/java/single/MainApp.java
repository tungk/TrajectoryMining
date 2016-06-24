package single;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
	    int O = Integer.parseInt(args[5]);
	    int T = Integer.parseInt(args[6]);
//	    int pattern_class = Integer.parseInt(args[7]);
	    String file_name = args[7];
	  
	    long time_start;
	    long time_end;
	    time_start = System.currentTimeMillis();
	    ArrayList<Trajectory> trajs = loadData(file_name);
	    time_end = System.currentTimeMillis();
	    System.out.println("Data Reading: " + (time_end-time_start) + " ms");
	    time_start = System.currentTimeMillis();
	    ArrayList<SnapShot> snapshots;
//	    if (n == 0) {
//		// o is used for restricting number of objects
//		snapshots = transformSnapKeepO(trajs, o);
//	    } else {
//		// o is used for restricting number of snapshots
//		snapshots = transformSnapKeepT(trajs, o);
//	    }
	    snapshots = transformSnapKeepOT(trajs, O, T);
	    
	    
	    time_end = System.currentTimeMillis();
	    System.out.println("Data Transformation: "
		    + (time_end - time_start) + " ms");
	    // snapshots are then feed to each pattern miner;
	    int[] class_masks = new int[]{0,1,2,4};
	    for(int i : class_masks) {
		  PatternMiner pm = (PatternMiner) classes[i]
			    .newInstance();
		    pm.loadParameters(e, p, m, k, l);
		    pm.loadData(snapshots);
		    pm.patternGen();
	    }
	}
    }
    
    private static ArrayList<SnapShot> transformSnapKeepOT( ArrayList<Trajectory> trajs, int max_o, int max_t) {
	TreeMap<Integer, SnapShot> ts_shot_map = new TreeMap<>();
	int o_count = 0, t_count = 0;  
	for (int i = 0; i < max_o && i < trajs.size(); i++) {
	    Trajectory traj = trajs.get(i);
	    int oid = traj.getID();
	    for (TemporalPoint tp : traj) {
		int t = tp.getTime();
		if (!ts_shot_map.containsKey(t)) {
		    ts_shot_map.put(t, new SnapShot(t));
		}
		ts_shot_map.get(t).addObject(oid, tp);
	    }
	    o_count++;
	}
	ArrayList<SnapShot> result = new ArrayList<>();
	for(int j = 0; j < max_t && !ts_shot_map.isEmpty(); j++) {
	    int key = ts_shot_map.firstKey();
	    result.add(ts_shot_map.get(key));
	    ts_shot_map.remove(key);
	    t_count++;
	}
	System.out.printf("Input data size %d objects, %d snapshots \n",
		o_count, t_count);
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
		.println("[Usage]: java -cp TrajectoryMining-0.0.1-SNAPSHOT-jar-with-depedencies.jar single.MainApp E P M K L MAX_O MAX_T Input");
    }
}
