package baseline;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import conf.AppProperties;

public class MainApp {
    public static void main(String[] args) {
    	SparkConf conf = new SparkConf();
	if (!conf.contains("spark.app.name")) {
	    conf = conf.setAppName(AppProperties.getProperty("appName"));
	}
	if (!conf.contains("spark.master")) {
	    conf = conf.setMaster(AppProperties.getProperty("spark_master"));
	}
	JavaSparkContext context = new JavaSparkContext(conf);
	BaselineLayout bl = new BaselineLayout(context);
	bl.runLogic();
    }
}
