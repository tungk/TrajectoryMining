package model;
/**
 *  A temporal point is a point in trajectory
 *  it is an immutable point with a time extension
 * @author a0048267
 *
 */
public class TemporalPoint extends Point{
    private static final long serialVersionUID = 4837777569308584367L;
    private final int time;
    private final int hashCode;
    public TemporalPoint(double lat, double lont, int time) {
	super(lat, lont);
	this.time = time;
	final int prime = 31;
	int result = super.hashCode();
	hashCode = prime * result + time;
    }
    public int getTime() {
        return time;
    }
    
    @Override
    public int hashCode() {
	return hashCode;
    }
    @Override
    public boolean equals(Object obj) {
	if(this == obj) {
	    return true;
	}
	if(obj instanceof TemporalPoint) {
	    if(((TemporalPoint) obj).getTime() == time) {
		if(Math.abs(((TemporalPoint) obj).getLat() - this.getLat()) < 0.0000001) {
		    if(Math.abs(((TemporalPoint) obj).getLont() - this.getLont()) < 0.0000001) {
			return true;
		    }
		}
	    }
	}
	return false;
    }
    
    @Override
    public String toString() {
	return String.format("<%8.6f,%8.6f,%d>", this.getLat(),
		this.getLont(), this.getTime());
    }
}