package algo;
/**
 * THIS SHOULD NOT BE RUN IN SPARK-1.6.2 or 1.5.2
 * In Spark 2.0.0, please uncomment the following
*/

/*
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.List;

import model.SnapshotClusters;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import schema.Schemas;

public class LineSweepAgg extends UserDefinedAggregateFunction {
	private static final long serialVersionUID = -2435499452636012775L;
	
	private StructType myInputSchema;
	private StructType myBufferSchema;
//	private DataType myReturnType;
	private LocalMiner myMiner;
	public LineSweepAgg(int k, int m, int l, int g) {
		myInputSchema = Schemas.getAggInputSchema();
//		myReturnType = Schemas.getAggInputSchema(); //just a string
		myBufferSchema = Schemas.getBufferSchema();
		myMiner = new LocalMiner(k,m,l,g);
	}
	
	@Override
	public StructType bufferSchema() {
		return myBufferSchema;
	}

	@Override
	public DataType dataType() {
		return DataTypes.StringType;
	}

	@Override
	public boolean deterministic() {
		return true;
	}

	@Override
	public String evaluate(Row buffer) {
		String everything = buffer.getString(0);
		System.out.println("The final buffer size:" + everything.length()+":"+everything);
		String[] snapshots = everything.split("=");
		ArrayList<SnapshotClusters> snaps = new ArrayList<>();
		//snapshots[0] = "";
		for(int i = 1; i < snapshots.length; i++) {
			String composed = snapshots[i];
			String[] decomp = composed.split("-");
			snaps.add(new SnapshotClusters(Integer.parseInt(decomp[0]), decomp[1]));
		}
		try {
			System.out.println("Inputs to miner: " + snaps.size());
			ArrayList<IntSet> patterns = myMiner.call(snaps);
			StringBuilder sb = new StringBuilder();
			for(IntSet pattern : patterns) {
				for(int j : pattern) {
					sb.append(j);
					sb.append(',');
				}
				sb.deleteCharAt(sb.length() -1);
				sb.append('\t');
			}
			if(sb.length() > 1) {
				sb.deleteCharAt(sb.length() -1);
			}
			return sb.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}

	@Override
	public void initialize(MutableAggregationBuffer arg0) {
		arg0.update(0, "");
	}

	@Override
	public StructType inputSchema() {
		return myInputSchema;
	}

	@Override
	public void merge(MutableAggregationBuffer arg0, Row arg1) {
//		arg0.getList(1).add(arg1.getString(0));
		arg0.update(0, arg0.getString(0) + "="+arg1.getString(0));
	}

	@Override
	public void update(MutableAggregationBuffer arg0, Row arg1) {
//		arg0.getList(1).add(arg1.getString(0));
//		List<String> a = arg0.getList(1);
//		a.add(arg1.getString(0));
//		arg0.update(1, a);
		arg0.update(0, arg0.getString(0) + "="+arg1.getString(0));
	}

}
*/
