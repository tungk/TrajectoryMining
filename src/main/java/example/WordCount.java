package example;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class WordCount {
  private static final FlatMapFunction<String, String> WORDS_EXTRACTOR =
      new FlatMapFunction<String, String>() {
	private static final long serialVersionUID = 1L;
	public Iterable<String> call(String s) throws Exception {
          return Arrays.asList(s.split(" "));  
        }
      };

  private static final PairFunction<String, String, Integer> WORDS_MAPPER =
      new PairFunction<String, String, Integer>() {
	private static final long serialVersionUID = 1L;
        public Tuple2<String, Integer> call(String s) throws Exception {
            if(s.endsWith(".")  || 
        	s.endsWith(",") ||
        	s.endsWith(":")) {
        	s = s.substring(0,s.length()-1);
            }
            if(s.endsWith("s")) {
        	s = s.substring(0,s.length()-1);
            }
            if(s.endsWith("ies")) {
        	s = s.substring(0, s.length()-3 ) +"y";
            }
          return new Tuple2<String, Integer>(s.toLowerCase(), 1);
        }
      };

  private static final Function2<Integer, Integer, Integer> WORDS_REDUCER =
      new Function2<Integer, Integer, Integer>() {
	private static final long serialVersionUID = 1L;
        public Integer call(Integer a, Integer b) throws Exception {
          return a + b;
        }
      };

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
    JavaSparkContext context = new JavaSparkContext(conf);
    JavaRDD<String> file = context.textFile("hdfs://cloud2.d1.comp.nus.edu.sg:9000/usr/fanqi/data/words");
    JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
    JavaPairRDD<String, Integer> pairs = words.mapToPair(WORDS_MAPPER);
    JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);
//    counter.saveAsTextFile("hdfs://cloud2.d1.comp.nus.edu.sg:9000/usr/fanqi/output/words/count");
//    Map<String, Integer> result = counter.collectAsMap();
//    for(String key : result.keySet()) {
//	System.out.println(key+"\t"+result.get(key));
//    }
    counter.saveAsHadoopFile("hdfs://cloud2.d1.comp.nus.edu.sg:9000/usr/fanqi/output/words/count",
	    String.class, Integer.class,WordCountOutputFormat.class);
    
    
    context.close();
  }
}

class WordCountOutputFormat implements OutputFormat<String, Integer>{
   class WordCountRecordWriter implements RecordWriter<String, Integer>{
    private DataOutputStream out;

    public WordCountRecordWriter(DataOutputStream iout) {
	out = iout;
    }
    @Override
    public void write(String key, Integer value) throws IOException {
	out.writeBytes("Key:<" + key + "> appears " + value + " times.\n");
    }

    @Override
    public void close(Reporter reporter) throws IOException {
    }
   }
    
    @Override
    public RecordWriter<String, Integer> getRecordWriter(FileSystem ignored,
	    JobConf job, String name, Progressable progress) throws IOException {
	Path file = FileOutputFormat.getTaskOutputPath(job, name);
	FileSystem fs = file.getFileSystem(job);
	FSDataOutputStream iout = fs.create(file,progress);
	return new WordCountRecordWriter(iout);
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job)
	    throws IOException {
	
    }
    
}