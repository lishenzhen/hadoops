package topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import topk.step.WordCount;
import topk.step.WordTopK;

public class TopKDriver {
	
	public static Logger logger = LoggerFactory.getLogger(TopKDriver.class);

	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node4:9000");
		conf.set("N", "10");
		
	    if (submitStep1(conf)) {
			logger.info("step 1 is ok.");
			conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://node4:9000");
			conf.set("N", "10");
			if (submitStep2(conf)) {
				logger.debug("step 2 is ok.");
			}
		}
	}
	
	public static boolean submitStep1(Configuration conf)throws Exception{
		
		Job job =new Job(conf);
		job.setJobName("topk-step1");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(WordCount.WordCountMapper.class);
		job.setReducerClass(WordCount.WordCountReducer.class);
		
		FileInputFormat.setInputPaths(job, new Path("/data/input/wordcount"));
	    FileOutputFormat.setOutputPath(job, new Path("/tmp/topk/wordcount"));
	    
	    return job.waitForCompletion(true);
	}

	public static boolean submitStep2(Configuration conf)throws Exception{
		
		Job job =new Job(conf);
		job.setJobName("topk-step2");
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(WordTopK.WordTopKMapper.class);
		job.setReducerClass(WordTopK.WordTopKRedeucer.class);
		
		FileInputFormat.setInputPaths(job, new Path("/tmp/topk/wordcount"));
	    FileOutputFormat.setOutputPath(job, new Path("/tmp/topk/wordcount"));
	    
	    return job.waitForCompletion(true);
	}
}
