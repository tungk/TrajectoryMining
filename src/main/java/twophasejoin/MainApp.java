package twophasejoin;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
   	Logger.getLogger("org").setLevel(Level.OFF);
   	Logger.getLogger("aka").setLevel(Level.OFF);
   	JavaSparkContext context = new JavaSparkContext(conf);
   	int hdfs_read_partitions = Integer.parseInt(AppProperties
   		.getProperty("hdfs_read_partitions"));
   	String hdfs_input = AppProperties.getProperty("hdfs_input");
   	TwoPhaseLayout lo = new TwoPhaseLayout(context.textFile(hdfs_input, hdfs_read_partitions));
   	lo.runLogic();
   	context.close();
       }
}
