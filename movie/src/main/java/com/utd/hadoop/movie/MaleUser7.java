package com.utd.hadoop.movie;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaleUser7 {
	//maper, filter age, output <age, userid::gender>
	public static class AgeMapper
    extends Mapper<LongWritable, Text, IntWritable, Text>{
		private final static IntWritable seven = new IntWritable(7);
		private IntWritable age = new IntWritable();
		private Text idGender = new Text();
		public void map(LongWritable key, Text value, Context context
		                 ) throws IOException, InterruptedException {
			String line = value.toString();
			String[] items = line.split("::");
			age.set(Integer.parseInt(items[2]));
			if (age.compareTo(seven)==0) {
				idGender.set(items[0]+"::"+items[1]);
				context.write(age, idGender);
			}
		}
	}
	
	//reducer, filter male user
	public static class GenderFiltReducer
	extends Reducer<IntWritable,Text,IntWritable,Text> {
		private Text result = new Text();

		public void reduce(IntWritable key, Iterable<Text> values,
                      Context context
                      ) throws IOException, InterruptedException {
			for (Text val: values) {
				String s[] = val.toString().split("::");				
				if (s.length == 2) {
					if (s[1].compareTo("M")==0) {
						result.set(val);
						context.write(key, result);
					}
				}				
			}
		}
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "male user 7 age filt");
	    job.setJarByClass(MaleUser7.class);
	    
	    job.setMapperClass(AgeMapper.class);
	    job.setCombinerClass(GenderFiltReducer.class);
	    job.setReducerClass(GenderFiltReducer.class);
	    
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
