package single;

import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;

import model.SnapShot;
import model.TemporalPoint;
import model.Trajectory;

/*
 * given a $G$, fix $L$, $K$, $M$, compare the number of patterns
 * 
 */
public class PatternCount {

    public static HashMap<Integer, IntSortedSet> obj_times = new HashMap<>();

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {
	if (args.length < 10) {
	    printHelper();
	    System.exit(-1);
	} else {
	    int e = Integer.parseInt(args[0]);
	    int p = Integer.parseInt(args[1]);
	    int m = Integer.parseInt(args[2]);
	    int k = Integer.parseInt(args[3]);
	    int l = Integer.parseInt(args[4]);
	    int g = Integer.parseInt(args[5]);
	    int r = Integer.parseInt(args[6]);
	    int o = Integer.parseInt(args[7]);
	    int t = Integer.parseInt(args[8]);
	    // int pattern_class = Integer.parseInt(args[7]);
	    String file_name = args[9];

	    long time_start;
	    long time_end;
	    time_start = System.currentTimeMillis();
	    ArrayList<Trajectory> trajs = loadData(file_name);
	    time_end = System.currentTimeMillis();
	    System.out.println("Data Reading: " + (time_end - time_start)
		    + " ms");
	    time_start = System.currentTimeMillis();
	    ArrayList<SnapShot> snapshots;
	    // if (n == 0) {
	    // // o is used for restricting number of objects
	    // snapshots = transformSnapKeepO(trajs, o);
	    // } else {
	    // // o is used for restricting number of snapshots
	    // snapshots = transformSnapKeepT(trajs, o);
	    // }
	    snapshots = transformSnapKeepOT(trajs, o, t);

	    time_end = System.currentTimeMillis();
	    System.out.println("Data Transformation: "
		    + (time_end - time_start) + " ms");
	    // snapshots are then feed to each pattern miner;
	    SwarmPatternWTimestamps spm = new SwarmPatternWTimestamps();
	    spm.loadParameters(e, p, m, k, r);
	    spm.loadData(snapshots);
	    spm.patternGen();
	    ArrayList<Pair<ArrayList<Integer>, ArrayList<Integer>>> result = spm
		    .getTemporalPatterns();
	    // count result
	    int swarms = 0;
	    int platoons = 0;
	    int convoys = 0;
	    int[] gcps = new int[10];
	    int[] gcps2 = new int[10];

	    for (Pair<ArrayList<Integer>, ArrayList<Integer>> times : result) {
		ArrayList<Integer> temporals = times.getRight();
		if (temporals.size() >= k) {
		    swarms++;
		    ArrayList<ArrayList<Integer>> cons = new ArrayList<>();
		    ArrayList<Integer> con = new ArrayList<>();
		    int con_size = 0;
		    boolean isconvoy = false;
		    for (int i = 1; i < temporals.size(); i++) {
			int diff = temporals.get(i) - temporals.get(i - 1);
			con.add(temporals.get(i - 1));
			if (diff != 1) {
			    if (con.size() >= l) {
				cons.add(con);
				con_size += con.size();
				if (con.size() >= k) {
				    isconvoy = true;
				}
			    }
			    con = new ArrayList<>();
			}
		    }
		    if (con.size() >= l) {
			cons.add(con);
			con_size += con.size();
			if (con.size() >= k) {
			    isconvoy = true;
			}
		    }
		    if (isconvoy) {
			convoys++;
		    }
		    // cons contains all the consecutive segements whose local
		    // >=l;
		    if (con_size >= k) {
			platoons++;
			// count different Gs
			for (int G = 1; G < 100; G += 10) {
			    int accum = cons.get(0).size();
			    boolean changed = false;
			    for (int c = 1; c < cons.size(); c++) {
				if (cons.get(c).get(0)
					- cons.get(c - 1).get(
						cons.get(c - 1).size() - 1) > G) {
				    if (accum >= k) {
					gcps[G / 10]++;
					changed = true;
					break;
				    } else {
					accum = cons.get(c).size();
				    }
				} else {
				    accum += cons.get(c).size();
				}
			    }
			    if (!changed) {
				if (accum >= k) {
				    gcps[G / 10]++;
				}
			    }
			}
		    }

		    for (int G = 1; G < 100; G += 10) {
			int accum = 1;
			boolean changed = false;
			for (int i = 1; i < temporals.size(); i++) {
			    int diff = temporals.get(i) - temporals.get(i - 1);
			    if (diff <= G) {
				accum++;
			    } else {
				if (accum >= k) {
				    gcps2[G / 10]++;
				    changed = true;
				    break;
				} else {
				    accum = 0;
				}
			    }
			}
			if(!changed) {
			    if(accum >=k) {
				 gcps2[G / 10]++;
			    }
			}
		    }
		}
	    }

	    System.out
		    .printf("Swarms: %d\tPlatoons: %d\tConvoys: %d\n",
			    swarms, platoons, convoys);
	    for(int gg : gcps) {
		System.out.print(gg+"\t");
	    }
	    System.out.println();
	    for(int gg : gcps2) {
		System.out.print(gg+"\t");
	    }
	    System.out.println();
	}
    }

    private static ArrayList<SnapShot> transformSnapKeepOT(
	    ArrayList<Trajectory> trajs, int max_o, int max_t) {
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
	for (int j = 0; j < max_t && !ts_shot_map.isEmpty(); j++) {
	    int key = ts_shot_map.firstKey();
	    result.add(ts_shot_map.get(key));
	    ts_shot_map.remove(key);
	    t_count++;
	}
	System.out.printf("Input data size %d objects, %d snapshots \n",
		o_count, t_count);
	return result;
    }

    public static ArrayList<SnapShot> transformSnapKeepO(
	    ArrayList<Trajectory> trajs, int max_o) {
	TreeMap<Integer, SnapShot> ts_shot_map = new TreeMap<>();
	int o_count = 0, t_count = 0;
	for (int i = 0; i < max_o; i++) {
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
	while (!ts_shot_map.isEmpty()) {
	    int key = ts_shot_map.firstKey();
	    result.add(ts_shot_map.get(key));
	    ts_shot_map.remove(key);
	    t_count++;
	}
	System.out.printf("Input data size %d objects, %d snapshots \n",
		o_count, t_count);
	return result;
    }

    /**
     * Only keeps max_t temporals and max_o objects
     * 
     * @param trajs
     * @param max_t
     * @param max_o
     * @return
     */
    public static ArrayList<SnapShot> transformSnapKeepT(
	    ArrayList<Trajectory> trajs, int max_t) {
	TreeMap<Integer, SnapShot> ts_shot_map = new TreeMap<>();
	int o_count = 0, t_count = 0;
	for (Trajectory traj : trajs) {
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
	for (int i = 0; i < max_t; i++) {
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
