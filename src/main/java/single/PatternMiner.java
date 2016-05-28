package single;

import java.util.ArrayList;

import model.SnapShot;

/**
 * the interface for mining the patterns in related works
 * all of then are single machine based
 * @author a0048267
 */
public interface PatternMiner {
    public void patternGen();

    public void loadParameters(int ... data);

    public void loadData(final ArrayList<SnapShot> snapshots);

    public void printStats();
}	
