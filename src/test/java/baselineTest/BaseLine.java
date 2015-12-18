package baselineTest;

import olpartitioned.BaselineLayout;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import conf.AppProperties;

public class BaseLine {
    public static void main(String[] args) {
	SparkConf conf = new SparkConf();
	if (!conf.contains("spark.app.name")) {
	    conf = conf.setAppName(AppProperties.getProperty("appName"));
	}
	if (!conf.contains("spark.master")) {
	    conf = conf.setMaster(AppProperties.getProperty("spark_master"));
	}
	JavaSparkContext context = new JavaSparkContext(conf);
	int hdfs_read_partitions = Integer.parseInt(AppProperties
		.getProperty("hdfs_read_partitions"));
	String hdfs_input = AppProperties.getProperty("hdfs_input");
	BaselineLayout bl = new BaselineLayout(context.textFile(hdfs_input, hdfs_read_partitions));
	bl.runLogic();
	context.close();
    }
}
