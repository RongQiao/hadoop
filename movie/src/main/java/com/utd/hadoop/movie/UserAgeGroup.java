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


public class UserAgeGroup {
	//maper, filter age, output <age, gender 1>
	public static class AgeMapper
    extends Mapper<LongWritable, Text, IntWritable, Text>{
		private IntWritable age = new IntWritable();
		private Text genderCnt = new Text();
		public void map(LongWritable key, Text value, Context context
		                 ) throws IOException, InterruptedException {
			String line = value.toString();
			String[] items = line.split("::");
			age.set(Integer.parseInt(items[2]));

			genderCnt.set(items[1]+" "+"1");	//a number is 1
			context.write(age, genderCnt);

		}
	}
	
	//reducer, count gender, can be a combiner
	public static class AgeGenderReducer
	extends Reducer<IntWritable,Text,IntWritable,Text> {
		private Text resultM = new Text();
		private Text resultF = new Text();

		public void reduce(IntWritable key, Iterable<Text> values,
                      Context context
                      ) throws IOException, InterruptedException {
			int cntM = 0;
			int cntF = 0;
			for (Text val: values) {
				String s[] = val.toString().split(" ");				
				if (s.length == 2) {
					int num = Integer.parseInt(s[1]);
					if (s[0].compareTo("M")==0) {
						cntM += num;
					}
					else {
						cntF += num;
					}
				}				
			}
			resultM.set("M " + Integer.toString(cntM));
			resultF.set("F " + Integer.toString(cntF));
			context.write(key, resultM);
			context.write(key, resultF);
		}
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "user age gender group");
	    job.setJarByClass(UserAgeGroup.class);
	    
	    job.setMapperClass(AgeMapper.class);
	    job.setCombinerClass(AgeGenderReducer.class);
	    job.setReducerClass(AgeGenderReducer.class);
	    
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
