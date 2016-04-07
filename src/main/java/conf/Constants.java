package conf;

public class Constants {
    public static final double EARTH_RADIUS = 6371.01;
  //  int K = 40, L = 10, M = 10, G = 3;
    public static int K = 40;
    public static int L = 10;
    public static int M = 10;
    public static int G = 3;
    public static int EPS = 10;
    public static int MINPTS = 5;
    public static int SNAPSHOT_PARTITIONS = Integer.parseInt(AppProperties.getProperty("snapshot_partitions"));
}
