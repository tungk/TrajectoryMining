package single;

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang3.tuple.Pair;

import model.SimpleCluster;
import model.SnapShot;

public class PlatoonPattern implements PatternMiner {
    private int e, p, m,l, k;
   
    ArrayList<SnapShot> input;
    HashMap<Integer, IntSortedSet> obj_temporal;
    ArrayList<Pair<ArrayList<Integer>, IntSortedSet>> patterns;
    ArrayList<SimpleCluster> clusters;
    IntSortedSet Tmax;
    public PlatoonPattern() {
	obj_temporal = new HashMap<>();
	patterns = new ArrayList<>();
	clusters = new ArrayList<>();
	Tmax = new IntRBTreeSet();
    }
    
    
    @Override
    public void patternGen() {
	if (input == null) {
	    return;
	} else {
	    long time_start = System.currentTimeMillis();
	    for (SnapShot sp : input) {
		// DBSCANClustering
		DBSCANClustering dbscan = new DBSCANClustering(e, p, sp);
		int time = sp.getTS();
		for (SimpleCluster sc : dbscan.cluster()) {
		    clusters.add(sc);
		    // int cluster_id = Integer.parseInt(sc.getID());
		    for (Integer object : sc.getObjects()) {
			if (!obj_temporal.containsKey(object)) {
			    obj_temporal.put(object, new IntRBTreeSet());
			}
			obj_temporal.get(object).add(time);
		    }
		    Tmax.add(time);
		}
	    }
	    long time_end = System.currentTimeMillis();
	    System.out.println("[Platoon]-DBSCAN: " + (time_end - time_start)
		    + " ms");

	    // Object search starts from obj_temporal map
	    time_start = System.currentTimeMillis();
	    platoonMining();
	    time_end = System.currentTimeMillis();
	    System.out.println("[Platoon]-Mining: " + (time_end-time_start)+ " ms" + "\t Patterns:" + patterns.size());
	}
    }

    /**
     * Priori to Algorithm 1
     */
    private void platoonMining() {
	IntSortedSet T = new IntRBTreeSet(Tmax);
	int N = clusters.size();
	int s = 2;
	platoonMiner(clusters, new IntRBTreeSet(), T , N, s);
    }


    /**
     * This is Algorithm 1
     * @param clusters2
     * @param intRBTreeSet
     * @param tmax
     * @param n
     * @param s
     */
    private void platoonMiner(ArrayList<SimpleCluster> CDB,
	    IntSortedSet X, IntSortedSet T, int N, int s) {
	PrefixTable PT = new PrefixTable(T); //an empty Prefix Table
	for(SimpleCluster C : CDB) {
	    ArrayList<Integer> objs = new ArrayList<Integer>(C.getObjects());
//	    int o1st = objs.get(0);
	    insertTable(0, objs, PT);
	}
//	PT.printTable();
	IntSortedSet CP = new IntRBTreeSet(); // common prefix
	IntSortedSet RO = new IntRBTreeSet(); // remove objects
	
	for(int o : PT.getObjects()) {
	   Pair<IntSortedSet, Integer> RR = localConsecutiveSet(PT.getT());
	   if(RR.getRight() == N) {
	       CP.add(o);
	   } else  if(RR.getLeft().size() < k) {
	       RO.add(o);
	   }
	}
	RO.addAll(CP);
	if(CP.size() == 0) {
	    if(s == 2 && X.size() >= m) {
		if(T.size() > k && checkLocal(T)) {
		    patterns.add(Pair.of(new ArrayList<Integer>(X), T));
		}
	    }
	} else {
	    IntSortedSet temp = new IntRBTreeSet(CP);
	    temp.addAll(X);
	    if(temp.size() >= m) {
		if(T.size() > k && checkLocal(T)) {
		    patterns.add(Pair.of(new ArrayList<Integer>(temp), T));
		}
	    }
	}
	PT.removeObjects(RO);
	CP.addAll(X);
	suffixMerge(PT, CP);
    }
    

    private boolean checkLocal(IntSortedSet t) {
	ArrayList<Integer> tmp = new ArrayList<>(t);
	int con = 1;
	for(int i = 0; i < tmp.size() -1; i++) {
	    if(tmp.get(i+1) - tmp.get(i) != 1) {
		if(con < l) {
		    return false;
		}
		con = 1;
	    } else {
		con++;
	    }
	}
	return con >= l ;
    }


    private void suffixMerge(PrefixTable PT, IntSortedSet X) {
	ArrayList<Integer> objs = PT.getObjects();
	for(int i = objs.size() -1; i>=0; i--) {
	    if(i + X.size() < m) {
		break;
	    }
	    X.add(objs.get(i));
	    int s = subsetChecking(Pair.of(X, PT.getSmC(objs.get(i))));
	    if(s != 0) {
		ArrayList<PrefixList> Cdb = PT.getPLO(objs.get(i));
		ArrayList<SimpleCluster> cdb = new ArrayList<>();
		for(PrefixList pfl : Cdb) {
		    cdb.add(new SimpleCluster(pfl.getPrefix()));
		}
		platoonMiner(cdb, X, PT.getSmC(objs.get(i)), PT.getNcon(objs.get(i)), s);
	    }
	}
	return;
    }


    private int subsetChecking(Pair<IntSortedSet, IntSortedSet> X) {
	int s = 2;
	for(Pair<ArrayList<Integer>, IntSortedSet> Cp : patterns) {
	   if(Cp.getLeft().containsAll(X.getLeft()) 
	    && Cp.getRight().containsAll(X.getRight())
	    && X.getRight().containsAll(Cp.getRight())) {
	       s = 0;
	   }
	}
	return s;
    }


    private Pair<IntSortedSet, Integer> localConsecutiveSet(IntSortedSet t) {
	IntSortedSet SmC = new IntRBTreeSet();
	int Ncon = 0;
	IntSortedSet Tcon = new IntRBTreeSet();
	int c = 0;
	ArrayList<Integer> tmax = new ArrayList<>(t);
//	int first = tmax.get(0);
	int j = 0;
	for(int i = 0; i < tmax.size(); i++) {
	    if(i - j <= 1) {
		c++;
		if(i-j == 1) {
		    Tcon.add(tmax.get(i));
		}
	    } else {
		SmC.addAll(Tcon);
		Ncon += c;
		if(Tcon.size() >= l) {
		    Tcon.clear();
		    Tcon.add(tmax.get(i));
		    c = 1;
		}
	    }
	    j = i;
	}
	if(Tcon.size() >= l) {
	    SmC.addAll(Tcon);
	    Ncon += c;
	}
	return Pair.of(SmC,Ncon);
    }


    /**
     * This is Algorithm 2
     * @param o1st
     * @param objs
     * @param pT
     */
    private void insertTable(int o_index, ArrayList<Integer> C, PrefixTable PT) {
	if(C.size() <= o_index) {
	    //then stop;
	    return;
	}
	IntSortedSet p = new IntRBTreeSet();
	for(int i = 0; i < o_index; i++) {
	    p.add(C.get(i));
	}
	int o = C.get(o_index);
	IntSortedSet T = new IntRBTreeSet();
	for(Integer c : C) {
	    T.addAll(obj_temporal.get(c));
	}
	if(PT.containObject(o)) {
	    PT.TJoin(T);
	    PrefixList pfl = PT.inPLO(p, o);
	    if(pfl != null) {
		pfl.addT(T);
		pfl.addN(1);
	    }
	} else {
	    PT.insertPLO(o, p, T, 1);
	}
	if(o_index+1 < C.size()) {
	    insertTable(o_index + 1, C, PT);
	}
    }


    @Override
    public void loadParameters(int... data) {
	e = data[0];
	p = data[1];
	m = data[2];
	k = data[3];
	l = data[4];
	System.out.println("[PLATOON]-Parameters: " + "e=" + e + "\tp=" + p
		+ "\tm=" + m + "\tk=" + k + "\tl=" + l);
    }

    @Override
    public void loadData(ArrayList<SnapShot> snapshots) {
	input = new ArrayList<>();
	for(SnapShot ss : snapshots) {
	    input.add(ss.clone());
	}
	input = snapshots;
    }

    @Override
    public void printStats() {
	// TODO Auto-generated method stub
	
    }
    
    class PrefixTable {
	IntSortedSet suffix;
	IntSortedSet T;
	int N;
	
	ArrayList<Integer> objects;
	HashMap<Integer, IntSortedSet> SmcT;
	HashMap<Integer,Integer> Ncon;
	HashMap<Integer, ArrayList<PrefixList>> prefList;
	
	public PrefixTable() {
	    suffix = new IntRBTreeSet();
	    T = new IntRBTreeSet();
	    N = 0;
	    objects = new ArrayList<>();
	    SmcT = new HashMap<>();
	    Ncon = new HashMap<>();
	    prefList = new HashMap<>();
	}
	
	public int getNcon(Integer o) {
	    return Ncon.get(o);
	}

	public ArrayList<PrefixList> getPLO(Integer o) {
	    return prefList.get(o);
	}

	public IntSortedSet getSmC(Integer o) {
	    return SmcT.get(o);
	}

	/**
	 * remove every entries in RO
	 * @param rO
	 */
	public void removeObjects(IntSortedSet RO) {
	    for(int o : RO) {
		objects.remove(new Integer(o));
		SmcT.remove(o);
		Ncon.remove(o);
		prefList.remove(o);
	    }
	}

	public PrefixTable(IntSortedSet Tmax) {
	    this();
	    T.addAll(Tmax);
	}
	
	public IntSortedSet getT() {
	    return T;
	}
	
	public void TJoin(IntSortedSet T2) {
	    T.retainAll(T2);
	}

	public void insertPLO(int o, IntSortedSet p, IntSortedSet t2, int i) {
	    if(!Ncon.containsKey(o)) {
		Ncon.put(o, i);
	    }
	    if(!prefList.containsKey(o)) {
		prefList.put(o, new ArrayList<PrefixList>());
	    } 
	    prefList.get(o).add(new PrefixList(p, t2, i));
	    
	    if(!objects.contains(o)) {
		objects.add(o);
	    }
	    if(!SmcT.containsKey(o)) {
		SmcT.put(o, new IntRBTreeSet());
		SmcT.get(o).addAll(t2);
	    }
	}
	
	public ArrayList<Integer> getObjects() {
	    return objects;
	}


	public PrefixList inPLO(IntSortedSet p, int obj) {
	    if(prefList.containsKey(obj)) {
		ArrayList<PrefixList> prefixes = prefList.get(obj);
		for(PrefixList pfl : prefixes) {
		    if(pfl.getPrefix().containsAll(p)
			   && p.containsAll(pfl.getPrefix())) {
			return pfl;
		    } 
		}
		return null;
	    } else {
		return null;
	    }
	}

	public boolean containObject(int object) {
	    return objects.contains(object);
	}
	
	
	public void printTable() {
	    System.out.println("Suffix=" + suffix + "\t T="+T+"\tN="+N);
	    System.out.println("--------------------------------------------------");
	    for(int o : objects) {
		System.out.print(o+"\t");
		System.out.print(SmcT.get(o)+"\t");
		System.out.print(Ncon.get(o)+"\t");
		System.out.print(prefList.get(o)+"\t");
		System.out.println();
	    }
	}
    }
    
    class PrefixList {
	IntSortedSet prefix;
	IntSortedSet Tp;
	int Np;
	public PrefixList() {
	    prefix = new IntRBTreeSet();
	    Tp = new IntRBTreeSet();
	    Np = 0;
	}
	
	public PrefixList(IntSortedSet p, IntSortedSet t2, int i) {
	    this();
	    prefix.addAll(p);
	    Tp.addAll(t2);
	    Np = i;
	}

	public void addN(int i) {
	    Np += i;
	}

	public void addT(IntSortedSet t) {
	    Tp.addAll(t);
	}

	public IntSortedSet getPrefix() {
	    return prefix;
	}
	
	@Override
	public String toString() {
	  return String.format("<%s|%s|%d>", prefix, Tp, Np);
	}
    }
}