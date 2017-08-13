package topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
//	private static String fs_prefix= "hdfs://node4:9000";

	public static void main(String[] args) throws Exception{

		if (args.length != 3) {
			logger.warn("usage TopKDriver <N> <input> <output>");
	         System.exit(1);
	      }
		int N = Integer.parseInt(args[0]);
		String inputUrl = args[1];
		String outputUrl = args[2];
		
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", fs_prefix);
		conf.set("N", N+"");
		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path(inputUrl);
		Long taskId = System.currentTimeMillis();
		Path tmpPath = new Path("/tmp/topk/"+taskId);
		Path outputPath =new Path(outputUrl);
	    if (submitStep1(conf,inputPath,tmpPath)) {
			logger.info("TOPK PROGRESS: step 1 is OK.");
			if (submitStep2(conf,tmpPath,outputPath)) {
				fs.delete(tmpPath);
				logger.debug("TOPK PROGRESS: step 2 is OK.");
			}
		}
	}
	
	public static boolean submitStep1(Configuration conf,Path inputPath,Path outPath)throws Exception{
		
		Job job =new Job(conf);
		job.setJobName("topk-step1");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(WordCount.WordCountMapper.class);
		job.setReducerClass(WordCount.WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outPath);
	    
	    return job.waitForCompletion(true);
	}

	public static boolean submitStep2(Configuration conf,Path inputPath,Path outPath)throws Exception{
		
		Job job =new Job(conf);
		job.setJobName("topk-step2");
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(NullWritable.class);   
	   job.setMapOutputValueClass(Text.class);   
	      
	      // reduce()'s output (K,V)
	   job.setOutputKeyClass(IntWritable.class);
	   job.setOutputValueClass(Text.class);
	      
		job.setMapperClass(WordTopK.WordTopKMapper.class);
		job.setReducerClass(WordTopK.WordTopKRedeucer.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outPath);
	    
	    return job.waitForCompletion(true);
	}
	
}
