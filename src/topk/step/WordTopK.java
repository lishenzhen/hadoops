package topk.step;

import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import topk.bean.SumWord;

public class WordTopK {
		
	public static class WordTopKMapper extends Mapper<Text, IntWritable, NullWritable, Text>{
		
		private int N=10;
		private PriorityQueue<SumWord> top10Words= new PriorityQueue<>(N);
		
		@Override
		protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			SumWord sword = top10Words.peek();
			int count = value.get();
			if (sword!=null && count<=sword.getCount()) {
				return;
			}
			SumWord sw = new SumWord(count, key.toString());
			top10Words.poll();
			top10Words.add(sw);
		}
		
		@Override
		protected void setup(Mapper<Text, IntWritable, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			this.N = context.getConfiguration().getInt("N", 10); // default is top 10
		}
		
		@Override
		protected void cleanup(Mapper<Text, IntWritable, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for (SumWord sumWord : top10Words) {
				context.write(NullWritable.get(), new Text(sumWord.toString()));
			}
		}
	}
	
	public static class WordTopKRedeucer extends Reducer<NullWritable, Text, Text, IntWritable>{
		
		private int N=10;
		private PriorityQueue<SumWord> top10Words= new PriorityQueue<>(N);
		
		@Override
		protected void setup(Reducer<NullWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
		      this.N = context.getConfiguration().getInt("N", 10); // default is top 10
		}
		
		@Override
		protected void reduce(NullWritable key, Iterable<Text> values,
				Reducer<NullWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			SumWord sword =top10Words.peek();
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				Text text = (Text) it.next();
				String[] wordcount=text.toString().split(",");
				int count = Integer.parseInt(wordcount[1]);
				if (sword!= null && count<sword.getCount()) {
					continue;
				}
				SumWord sw = new SumWord(count, wordcount[0]);
				top10Words.poll();
				top10Words.add(sw);
			}
		}
		
		@Override
		protected void cleanup(Reducer<NullWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			for (SumWord sumWord : top10Words) {
				context.write( new Text(sumWord.getWord()),new IntWritable(sumWord.getCount()));
			}
		}
	}
	
}
