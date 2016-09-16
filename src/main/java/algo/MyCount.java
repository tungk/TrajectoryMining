package algo;

/**
 * THIS SHOULD NOT BE RUN IN SPARK-1.6.2 or 1.5.2
 * In Spark 2.0.0, please uncomment the following
*/

/*
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MyCount extends UserDefinedAggregateFunction {
	private static final long serialVersionUID = -6196420788688951643L;

	@Override
	public StructType bufferSchema() {
		List<StructField> schema = new ArrayList<>();
		schema.add(DataTypes.createStructField("count", DataTypes.IntegerType, false));
		return DataTypes.createStructType(schema);
	}

	@Override
	public DataType dataType() {
		return DataTypes.IntegerType;
	}

	@Override
	public boolean deterministic() {
		return true;
	}

	@Override
	public Integer evaluate(Row buffer) {
		return buffer.getInt(0);
	}

	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, 0);
	}

	@Override
	public StructType inputSchema() {
		List<StructField> schema = new ArrayList<>();
		schema.add(DataTypes.createStructField("clusters", DataTypes.StringType, false));
		return DataTypes.createStructType(schema);
	}

	@Override
	public void merge(MutableAggregationBuffer buffer, Row row) {
		buffer.update(0, buffer.getInt(0) + 1);
	}

	@Override
	public void update(MutableAggregationBuffer buffer, Row row) {
		buffer.update(0, buffer.getInt(0) + 1);
	}
}
*/