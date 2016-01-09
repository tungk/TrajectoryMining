package conf;

public class Constants {
    public static final double EARTH_RADIUS = 6371.01;
    public static final  int K = Integer.parseInt(AppProperties
	    .getProperty("K"));
    public static final  int L = Integer.parseInt(AppProperties
	    .getProperty("L"));
    public static final  int M = Integer.parseInt(AppProperties
	    .getProperty("M"));
    public static final  int G = Integer.parseInt(AppProperties
	    .getProperty("G"));
    public static final  int EPS = Integer.parseInt(AppProperties
	    .getProperty("eps"));
    public static final  int MINPTS = Integer.parseInt(AppProperties
	    .getProperty("minPts"));
    public static final  int SNAPSHOT_PARTITIONS = Integer
	    .parseInt(AppProperties.getProperty("snapshot_partitions"));

}
