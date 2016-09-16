package dataframe;
/**
 * THIS SHOULD NOT BE RUN IN SPARK-1.6.2 or 1.5.2
 * In Spark 2.0.0, please uncomment the following
*/

/*
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import model.SnapshotClusters;
import model.SnapshotRow;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import algo.LineSweepAgg;
import algo.MyCount;

import scala.Tuple2;
import schema.Schemas;

import conf.AppProperties;

public class MainApp {
	public static void main(String[] args) {
		int K = 40, L = 10, M = 10, G = 3;
		int ls_partition = 486;
		if (args.length > 0) {
		    for (String arg : args) {
			System.out.println(arg);
			if (arg.startsWith("k=") || arg.startsWith("K=")) {
			    K = Integer.parseInt(arg.split("=")[1]);
			} else if (arg.startsWith("l=") || arg.startsWith("L=")) {
			    L = Integer.parseInt(arg.split("=")[1]);
			} else if (arg.startsWith("m=") || arg.startsWith("M=")) {
			    M = Integer.parseInt(arg.split("=")[1]);
			} else if (arg.startsWith("g=") || arg.startsWith("G=")) {
			    G = Integer.parseInt(arg.split("=")[1]);
			} else if (arg.startsWith("c=") || arg.startsWith("C=")) {
			    ls_partition = Integer.parseInt(arg.split("=")[1]);
			}
		    }
		} else {
		    System.out
			    .println("Usage: ./bin/spark-submit --class dataframe.MainApp "
				    + "~/TrajectoryMining/TrajectoryMining-0.0.1-SNAPSHOT-jar-with-dependencies.jar "
				    + "k=40 l=10 g=3 h=195 e=15 p=10 c=115");
		    System.exit(5);
		}
		
		String hdfs_input = AppProperties.getProperty("hdfs_input");
		String name = "DataFrame-K" + K + "-L" + L + "-M" + M + "-G" + G
			+ "-File" + hdfs_input;
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("aka").setLevel(Level.OFF);

		SparkConf conf = new SparkConf().setAppName(name);
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<SnapshotClusters> CLUSTERS = context.objectFile(hdfs_input,
			ls_partition);

		JavaRDD<Row> rows = CLUSTERS
			.map(new Function<SnapshotClusters, Row>() {
			    private static final long serialVersionUID = 5448006220516859929L;
			    @Override
			    public Row call(SnapshotClusters v1)
				    throws Exception {
			    	return RowFactory.create(v1.getTimeStamp(), v1.getTimeStamp()+"-"+ v1.getStringFormatClusters());
			    }
			}).cache();
		
		//create schema
//		List<StructField> fields = new ArrayList<>();
//		fields.add(DataTypes.createStructField("key", DataTypes.IntegerType, false));
//		fields.add(DataTypes.createStructField("clusters", DataTypes.StringType, false));
//		StructType snapshotSchema = DataTypes.createStructType(fields);
		
		SparkSession sparksql = SparkSession.builder().config(conf).getOrCreate();
		Dataset<Row> ds = sparksql.createDataFrame(rows, Schemas.getSnapshotSchema());	
//		SQLContext sql = new SQLContext(context);
//		Dataset<Row> df = sql.createDataFrame(rows, SnapshotRow.class);
		System.out.println("DataSet size:" + ds.count());
		
		int eta = ((int) (Math.ceil(K * 1.0 / L)) - 1) * G + K + L - 1;
		System.out.println("Eta:" + eta);
		WindowSpec ws = Window.partitionBy("key").orderBy("key").rowsBetween(0, eta);
		MyCount mc = new MyCount();
		sparksql.udf().register("mycount", mc);
		Dataset<Row> sql_result =ds.select(ds.col("key"), functions.callUDF("mycount", ds.col("clusters")).over(ws).alias("pattern"));
//		LineSweepAgg lsa = new LineSweepAgg(K,M,L,G);
//		sparksql.udf().register("linesweep", lsa);
//		Dataset<Row> sql_result = ds.select(ds.col("key"), functions.callUDF("linesweep", ds.col("clusters")).over(ws).alias("pattern"));
//		System.out.println("Rows:\t"+sql_result.count());
		System.out.println("Query Plan");
		sql_result.explain();
		sql_result.show(5000);
//		JavaRDD<Row> result = sql_result.toJavaRDD();
//		result.repartition(ls_partition);
//		JavaRDD<IntSet> patterns = result.flatMap(new FlatMapFunction<Row, IntSet>() {
//			private static final long serialVersionUID = 1339734028710983710L;
//			@Override
//			public Iterator<IntSet> call(Row input) throws Exception {
//				String input_str = input.getString(1);
//				String[] parts = input_str.split("\t");
//				ArrayList<IntSet> result =  new ArrayList<>();
//				for(String part : parts) {
//					IntSet is = new IntOpenHashSet();
//					String[] objs = part.split(",");
//					for(String obj : objs) {
//						if(!obj.equals("")) {
//							is.add(Integer.parseInt(obj));
//						}
//					}
//					result.add(is);
//				}
//				return result.iterator();
//			}
//		}).filter(new Function<IntSet, Boolean>(){
//			private static final long serialVersionUID = 1357894559829247868L;
//			@Override
//			public Boolean call(IntSet arg0) throws Exception {
//				return !arg0.isEmpty();
//			}
//		}).cache();
//		
//		List<IntSet> total_patterns = patterns.collect();
//		
//		DuplicateClusterFilter duplicate_removal = new DuplicateClusterFilter(total_patterns);
//		
//		for(IntSet t_pattern :patterns.filter(duplicate_removal).collect()) {
//			System.out.println(t_pattern);
//		}
		context.close();
	}
}

class DuplicateClusterFilter implements Function<IntSet, Boolean> {
	private static final long serialVersionUID = -602382731006034424L;
	private final List<IntSet> grounds;

	public DuplicateClusterFilter(List<IntSet> ground) {
		grounds = ground;
	}

	@Override
	public Boolean call(IntSet v1) throws Exception {
		for (IntSet gr : grounds) {
			if (gr.containsAll(v1) && gr.size() > v1.size()) { // ensures
				return false;
			}
		}
		return true;
	}
}
*/
