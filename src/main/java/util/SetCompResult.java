package util;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class SetCompResult implements Serializable {
    private static final long serialVersionUID = 3428638190351453626L;
    Set<Integer> commons;
    int status;
    public SetCompResult() {
	commons = new HashSet<Integer>();
	status = 0;
    }
    
    public int getStatus() {
	return status;
    }
    
    public void addCommons(int e) {
	commons.add(e);
    }
    
    public int getCommonsSize() {
	return commons.size();
    }
    
    public Set<Integer> getCommons() {
	return commons;
    }
    
    /**
     * 0: no containment
     * 1: s1 contains s2
     * 2: s2 contains s1
     * 3: s1 equals s2
     * @param status
     */
    public void setStatus(int status) {
	this.status = status;
    }
    
    @Override
    public String toString() {
	return "<"+status+":"+commons+">";
    }
}