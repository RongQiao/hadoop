package com.utd.hadoop.movie;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MovieGenre {
	//maper, filter move genre, output <genre, title>
	public static class GenreMapper
    extends Mapper<LongWritable, Text, Text, Text>{
		private String strQuery;
		private Text genre;
		public void setup(Context context) {
			strQuery = context.getConfiguration().get("utd.hadoop.movie.query");
			genre = new Text(strQuery);
		}

		public void map(LongWritable key, Text value, Context context
		                 ) throws IOException, InterruptedException {
			String[] items = value.toString().split("::");
			if (items.length == 3) {
				String allGenre = items[2].toLowerCase();

				if (allGenre.indexOf(strQuery)>=0) {
					context.write(genre, new Text(items[1]));
				}
			}
			
		}
	}
	
	//reducer, directly out put
	public static class GenreReducer
	extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,
                      Context context
                      ) throws IOException, InterruptedException {
			for (Text val:values) {
				context.write(key, val);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    if (args.length >= 3) {
	    	conf.set("utd.hadoop.movie.query", args[2].toLowerCase());
	    }
	    Job job = Job.getInstance(conf, "movie genre title");
	    job.setJarByClass(MovieGenre.class);
	    
	    job.setMapperClass(GenreMapper.class);
	    job.setReducerClass(GenreReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    if (args.length >= 3) {
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));		    	    
	    }
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
