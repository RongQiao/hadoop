package com.utd.hadoop.movie;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Requirement: Find top 5 average movies rated by female users and print out the titles and
the average rating given to the movie by female users.
 *
 * Data format:
 * movies.dat:
 * 		format:		MovieID::Title::Genres
 * 		example:	1::Toy Story (1995)::Animation|Children's|Comedy
 * users.dat
 * 		format:		UserID::Gender::Age::Occupation::Zip-code
 * 		example:	1::M::7::10::48007
 * ratings.dat
 * 		format:		UserID::MovieID::Rating::Timestamp
 * 		example: 	1::1193::5::978300760
 * 
 * Algorithm: using reduce-side join, chained job
 * job 1: join users.dat and ratings.dat
 * 		1. Mapper (2)
 * 			(1)filter female user from users.dat using UserMapper
 * 			(2)select <userID, MovieID, Rating> from ratings.dat using RatingMapper 
 * 		2. Reducer
 * 			join female Users and ratings using userID as join condition
 * job 2: join output of job 1 and movies.dat
 * 		1. Mapper
 * 			(1) output of job 1, calculate average rating of movies, output top 5, <MovieID, averageRating>
 * 			(2) select <MovieID, Title> from movies.dat	
 * 		5. Reducer
 * 			join 
 */
public class MovieRating {
	//abstract mapper, for using same split string for data format
	public static abstract class DataMapper
	extends Mapper<LongWritable, Text, Text, Text> {
		private String splitStr = new String();		
		public void setup(Context context) {
			splitStr = context.getConfiguration().get("data.split");
		}
		
		public String getSplit() {
			return splitStr;
		}
	}
	
	//mapper, filter gender, output <userid, "U">, "U" means the data is from users.dat
	public static class UserMapper
    extends DataMapper{
		private Text userId = new Text();	
		private Text dataSource = new Text("U");
		/*
		 * input: key - line offset, value - content of line
		 * output: key - UserID, value - NullWritable
		 * algorithm: filter Gender=="F"
		 */
		public void map(LongWritable key, Text value, Context context
		                 ) throws IOException, InterruptedException {
			String line = value.toString();
			String[] items = line.split(getSplit());
			
			if (items.length >= 2){
				//items[0]: userId; items[1]: Gender "F"/"M"
				if (items[1].compareTo("F")==0) {
					userId.set(items[0]);
					context.write(userId, dataSource);
				}
			}

		}
	}
	
	//mapper, output<userID, "R"::MovieID::Rating>, "R" means the data is from rating.dat
	public static class RatingMapper
    extends DataMapper{
		private Text userId = new Text();
		private Text movieRating = new Text();
		
		/*
		 * input: key - line offset, value - content of line
		 * output: key - UserID, value -  MovieID::Rating
		 */
		public void map(LongWritable key, Text value, Context context
		                 ) throws IOException, InterruptedException {
			String line = value.toString();
			String[] items = line.split(getSplit());
			
			if (items.length >= 3){
				//items[0]: userId; items[1]: MovieID; items[2]: rating
				userId.set(items[0]);
				
				StringBuilder str = new StringBuilder();
				str.append("R");
				str.append(getSplit());
				str.append(items[1]);
				str.append(getSplit());
				str.append(items[2]);
				
				movieRating.set(str.toString());
				context.write(userId, movieRating);
			}
		}
	}
	
	//reducer, output<MovieID, Rating>
	public static class JoinUserRatingReducer
	extends Reducer<Text,Text,Text,FloatWritable> {
		//for out put
		private Text movieText = new Text();
		private FloatWritable ratingFloat = new FloatWritable();
		//for join
		private List<String> recordUser = new ArrayList<String>();
		private List<MovieRatingRecord> recordRating = new ArrayList<MovieRatingRecord>();
		private String getSplit() {
			return "::";
		}
	
		public void reduce(Text key, Iterable<Text> values,
                      Context context
                      ) throws IOException, InterruptedException {
			recordUser.clear();
			recordRating.clear();
			
			String userId = key.toString();
			for (Text val: values) {
				String rc = val.toString();
				if (val.charAt(0) == 'U') {
					recordUser.add(userId);
				}
				else {
					String items[] = rc.split(getSplit());
					if (items.length >= 3) {
						MovieRatingRecord mrRc = new MovieRatingRecord();
						mrRc.setUserId(userId);
						mrRc.setMovieId(items[1]);
						mrRc.setRating(Integer.parseInt(items[2]));
						recordRating.add(mrRc);
					}
					
				}
			}
			
			//join
			for (String usr: recordUser) {
				for (MovieRatingRecord mr: recordRating) {
					if (mr.getUserId().compareTo(usr)==0) {
						movieText.set(mr.getMovieId());
						ratingFloat.set(mr.getRating());
						context.write(movieText, ratingFloat);
					}
				}
			}
		}
		
		private class MovieRatingRecord{
			private String userId;
			private String movieId;
			private int rating;
			public String getUserId() {
				return userId;
			}
			public void setUserId(String userId) {
				this.userId = userId;
			}
			public String getMovieId() {
				return movieId;
			}
			public void setMovieId(String movieId) {
				this.movieId = movieId;
			}
			public int getRating() {
				return rating;
			}
			public void setRating(int rating) {
				this.rating = rating;
			}
		}
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("data.split", "::");	//all data format is using "::" as split string, so set it as a configuration 
	    
	    Job jobJoinUR = Job.getInstance(conf, "join user and rating");
	    jobJoinUR.setJarByClass(MovieRating.class);
	    jobJoinUR.setMapOutputKeyClass(Text.class);
	    jobJoinUR.setMapOutputValueClass(Text.class);
	    //use MultipleOutputs and specify different Record class and Input formats
	    MultipleInputs.addInputPath(jobJoinUR, new Path(args[0]), TextInputFormat.class, UserMapper.class);	   
	    MultipleInputs.addInputPath(jobJoinUR, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
		jobJoinUR.setReducerClass(JoinUserRatingReducer.class);	
		jobJoinUR.setOutputKeyClass(Text.class);
		jobJoinUR.setOutputValueClass(Text.class);
	    
	    FileOutputFormat.setOutputPath(jobJoinUR, new Path(args[2]));
	    
	    System.exit(jobJoinUR.waitForCompletion(true) ? 0 : 1);
	}
}
