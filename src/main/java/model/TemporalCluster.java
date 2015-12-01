package model;

import java.io.Serializable;

public class TemporalCluster implements Serializable {
    private static final long serialVersionUID = 3303646184247058466L;
    private int ts;
    private String CID;
    public TemporalCluster(int its, String ID) {
	ts = its;
	CID = ID;
    }
    
    public int getTS() {
	return ts;
    }
    
    public String getID() {
	return CID;
    }
}
