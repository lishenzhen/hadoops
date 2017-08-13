package topk.step;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		static final IntWritable count = new IntWritable(1);
		static final Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String words = value.toString().toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer(words,"\t\n\f\" .,:;?![]'-=");
			while (tokenizer.hasMoreElements()) {
				String word = (String) tokenizer.nextElement();
				WordCountMapper.word.set(word);
				context.write(WordCountMapper.word, count);
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private IntWritable result= new IntWritable(0);
		@Override
		protected void reduce(Text text, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			Iterator<IntWritable> it = values.iterator();
			int sum = 0;
			while (it.hasNext()) {
				IntWritable count = it.next();
				sum += count.get();
			}
			result.set(sum);
			context.write(text, result);
		}
	}

	
}
