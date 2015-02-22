package com.utd.hadoop.movie;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
 * job 2: calculate average rating of movies, output top 5, <averageRating, list<MovieID>>
 * 		Mapper | Reducer | Mapper chain
 * 		1. Mapper
 * 			TextInputFile -> <MovieID, rating>
 * 		2. Reducer
 * 			a. reduce - for each movie, calculate sum of rating, using TreeMap to store the sum and count
 * 			b. cleanup - choose top 5 rating from TreeMap, for one rating value, it may include multi-movies  
 * 		3. Mapper
 * 			only choose 5 movie in the top rating to emit, similar with reducer
 * job 3: join output of job 1 and movies.dat
 * 		1. Mapper
 * 			(1) output of job 2, choose top 5 movie
 * 			(2) select <MovieID, Title> from movies.dat	
 * 		5. Reducer
 * 			join top5 and movies using MovieID as join condition
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
	
	/*
	 * for job 1
	 */
	//mapper, filter gender, output <userid, "U">, "U" means the data is from users.dat
	public static class UserMapper
    extends DataMapper{
		private Text userId = new Text();	
		private Text dataSource = new Text("U");
		/*
		 * input: key - line offset, value - content of line
		 * output: key - UserID, value - "U"
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
	extends Reducer<Text,Text,Text,IntWritable> {
		//for out put
		private Text movieId = new Text();
		private IntWritable rating = new IntWritable();
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
						movieId.set(mr.getMovieId());
						rating.set(mr.getRating());
						context.write(movieId, rating);
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
	
	/*
	 * for job 2
	 */
	//mapper,
	public static class AvRatingMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text movieId = new Text("2396");	
		private IntWritable rating = new IntWritable(2);
		/*
		 * input: key - line offset, value - content of line
		 * output: key - movieId, value - rating
		 * algorithm: filter Gender=="F"
		 */
		public void map(LongWritable key, Text value, Context context
		                 ) throws IOException, InterruptedException {
			String line = value.toString();
			String[] items = line.split("\t");
			System.out.println("AvRatingMapper" + items.length);
			if (items.length >= 2){
				//items[0]: movieId; items[1]: rating value
				movieId.set(items[0]);
				rating.set(Integer.parseInt(items[1]));
				System.out.println("AvRatingMapper" + line + items[0] + items[1]);
			}
			
			context.write(movieId, rating);
		}
	}
		
	//mapper, 
	public static class Top5Mapper
    extends Mapper<NullWritable, Text, Text, Text> {
		//using TreeMap to sort rating, only emit the top 5 in this reducer
		private TreeMap<Float, List<String>> ratingMap = new TreeMap<Float, List<String>>();
		
		public void setup(Context context) {
			ratingMap.clear();
		}
		public void cleanup(Context context) throws IOException, InterruptedException {
			Text movieId = new Text();
			Text avRating = new Text();
			//choose top 5 rating value
			int cnt = 0;
			while (cnt < 5) {
				Entry<Float, List<String>> en = ratingMap.lastEntry();
				if (en != null) {
					for(String mid: en.getValue()) {
						movieId.set(mid);
						avRating.set(Float.toString(en.getKey()));
						context.write(movieId, avRating);	//only 5 movieId will be emitted
						cnt++;
						if (cnt >= 5) {
							break;
						}
					}
				}
				else {
					break;
				}
			}
		}
		
		/*
		 * input: key - line offset, value - content of line
		 * output: key - movieId, value - rating
		 * algorithm: filter Gender=="F"
		 */
		public void map(NullWritable key, Text value, Context context
		                 ) throws IOException, InterruptedException {
			String line = value.toString();
			String[] items = line.split("::");
			if (items.length >= 2){
				String ratingStr = items[0];
				Float rating = Float.parseFloat(ratingStr);
				List<String> movies;
				if (ratingMap.containsKey(rating)) {
					movies = ratingMap.get(rating);
				}
				else {
					movies = new ArrayList<String>();
				}
				for (int i = 1; i < items.length; i++) {
					movies.add(items[i]);
				}
				ratingMap.put(rating, movies);
			}
		}
	}
	
	//reducer, calculate average rating and select top 5 rating value, output <movieId, averageRating>
	public static class TopRatingReducer
	extends Reducer<Text, IntWritable, NullWritable, Text>{
		private String getSplit() {
			return "::";
		}
		//using TreeMap to sort rating, only emit the top 5 in this reducer
		private TreeMap<RatingPair, List<String>> ratingMap = new TreeMap<RatingPair, List<String>>(new RatingComp());
		
		public void setup(Context context) {
			ratingMap.clear();
		}
		public void cleanup(Context context) throws IOException, InterruptedException {
			//choose top 5 rating value
			for (int i = 0; i < 5; i++) {
				Entry<RatingPair, List<String>> en = ratingMap.lastEntry();
				if (en != null) {
					float avRt = (float)en.getKey().getSum() / (float)en.getKey().getCnt();
					
					//put all result into string
					StringBuilder strB = new StringBuilder();
					strB.append(avRt);
					for (String mId: en.getValue()) {
						strB.append(getSplit());
						strB.append(mId);
					}
					//emit
					context.write(NullWritable.get(), new Text(strB.toString()));
					//remove the current last entry
					ratingMap.remove(ratingMap.lastKey());
				}
			}
		}
		//calculate average rating for one movie
		public void reduce(Text key, Iterable<IntWritable> values,
                Context context
                ) throws IOException, InterruptedException {
			String movieId = key.toString();
			
			int sum = 0;			
			int cnt = 0;
			for (IntWritable rt: values) {
				sum += rt.get();
				cnt++;
			}
			
			//insert the rating to TreeMap 
			RatingPair rp = new RatingPair(sum, cnt);
			List<String> mvs;
			if (ratingMap.containsKey(rp)) {
				mvs = ratingMap.get(rp);				
			}
			else {
				mvs = new ArrayList<String>();			
			}
			mvs.add(movieId);
			ratingMap.put(rp, mvs);
		}
				
		//for store rating value, instead of saving float average value, saving sum and count, and customize comparator
		private class RatingPair{
			private int sum;
			private int cnt;
			public RatingPair(int sum, int cnt) {
				setSum(sum);
				setCnt(cnt);
			}
			public int getSum() {
				return sum;
			}
			public void setSum(int sum) {
				this.sum = sum;
			}
			public int getCnt() {
				return cnt;
			}
			public void setCnt(int cnt) {
				this.cnt = cnt;
			}
		}
		
		class RatingComp implements Comparator<RatingPair>{			 
		    public int compare(RatingPair r1, RatingPair r2) {
		    	int rt = 0;
		    	if ((r1.getSum()==r2.getSum()) && (r1.getCnt()==r2.getCnt())) {
		    		rt = 0;
		    	}
		    	else {
		    		float av1 = (float)r1.getSum() / (float)r1.getCnt();
		    		float av2 = (float)r2.getSum() / (float)r2.getCnt();
		    		if (av1 < av2) {
		    			rt = -1;
		    		}
		    		else {
		    			rt = 1;
		    		}
		    	}
		        return rt;
		    }
		}
		
	}
	
	/*
	 * for job 3
	 */
	//mapper, output<movieID, "T">, "T" means the data is from Top5 rating
	public static class Top5MovieIdMapper
    extends DataMapper{
		private Text movieID = new Text();
		private Text top5Rating = new Text("T");
		
		/*
		 * input: key - line offset, value - content of line
		 * output: key - MovieID, value -  "T"
		 */
		public void map(LongWritable key, Text value, Context context
		                 ) throws IOException, InterruptedException {
			String line = value.toString();
			
			if (line.length() > 0){
				movieID.set(line);
				context.write(movieID, top5Rating);
			}
		}
	}
	
	//mapper, output<movieId, "M"::Title>, M" means the data is from movies.dat
	public static class MoviesMapper
    extends DataMapper{
		private Text movieId = new Text();
		private Text movieTitle = new Text();
		
		/*
		 * input: key - line offset, value - content of line
		 * output: key - movieId, value -  "M"::Title
		 */
		public void map(LongWritable key, Text value, Context context
		                 ) throws IOException, InterruptedException {
			String line = value.toString();
			String[] items = line.split(getSplit());
			
			if (items.length >= 3){
				//items[0]: userId; items[1]: MovieID; items[2]: rating
				context.write(movieId, movieTitle);
			}
		}
	}


	private static boolean runJob1(Configuration conf, String input1, String input2, String output) throws Exception{
	    Job jobJoinUR = Job.getInstance(conf, "join user and rating");
	    jobJoinUR.setJarByClass(MovieRating.class);
	    //mapper
	    //use MultipleOutputs and specify different Record class and Input formats
	    MultipleInputs.addInputPath(jobJoinUR, new Path(input1), TextInputFormat.class, UserMapper.class);	   
	    MultipleInputs.addInputPath(jobJoinUR, new Path(input2), TextInputFormat.class, RatingMapper.class);
		//reducer	    
	    jobJoinUR.setReducerClass(JoinUserRatingReducer.class);	
		jobJoinUR.setOutputKeyClass(Text.class);
		jobJoinUR.setOutputValueClass(Text.class);
	    FileOutputFormat.setOutputPath(jobJoinUR, new Path(output));
	    boolean ret = jobJoinUR.waitForCompletion(true);
		return ret;
	}

	private static boolean runJob2(Configuration conf, String input, String output) throws Exception{
		Job jobAvRt = Job.getInstance(conf, "select top 5");
		jobAvRt.setJarByClass(MovieRating.class);
	    //mapper
		jobAvRt.setMapOutputKeyClass(Text.class);
		jobAvRt.setMapOutputValueClass(IntWritable.class);
		jobAvRt.setMapperClass(AvRatingMapper.class);
		//reducer	   
		ChainReducer.setReducer(jobAvRt, TopRatingReducer.class, 
				Text.class, IntWritable.class, NullWritable.class, Text.class, conf);
		ChainReducer.addMapper(jobAvRt, Top5Mapper.class, 
				NullWritable.class, Text.class, Text.class, Text.class, conf);
	    
		jobAvRt.setInputFormatClass(TextInputFormat.class);
	    FileInputFormat.setInputPaths(jobAvRt, new Path(input));
	    FileOutputFormat.setOutputPath(jobAvRt, new Path(output));
	    
	    boolean ret = jobAvRt.waitForCompletion(true);
	    return ret;
	}
	
	private static boolean runJob3(Configuration conf, String input1, String input2, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Job jobMvTitle = Job.getInstance(conf, "final: join movie title");
		jobMvTitle.setJarByClass(MovieRating.class);
	    //mapper
		//use MultipleOutputs and specify different Record class and Input formats
	    MultipleInputs.addInputPath(jobMvTitle, new Path(input1), TextInputFormat.class, Top5Mapper.class);	   
	    //MultipleInputs.addInputPath(jobMvTitle, new Path(input2), TextInputFormat.class, MovieMapper.class);
	    //reducer	    
	    jobMvTitle.setReducerClass(TopRatingReducer.class);	
	    jobMvTitle.setOutputKeyClass(NullWritable.class);
	    jobMvTitle.setOutputValueClass(Text.class);
	    
	    jobMvTitle.setInputFormatClass(TextInputFormat.class);
	    FileOutputFormat.setOutputPath(jobMvTitle, new Path(output));
	    
	    boolean ret = jobMvTitle.waitForCompletion(true);
	    return ret;
	}
	
	public static void main(String[] args) throws Exception {
		String jobInterMediate[] = {"tmp1", "tmp2"};
	    Configuration conf = new Configuration();
	    conf.set("data.split", "::");	//all data format is using "::" as split string, so set it as a configuration 
	    
	    //job 1
	    boolean ret = false;
	    ret = runJob1(conf, args[0], args[1], jobInterMediate[0]);	
//	    if (ret) {
	    	//job 2
	    	ret = runJob2(conf, jobInterMediate[0]+"/part-r-00000", jobInterMediate[1]);
	    	if (ret) {
	    		//ret = runJob3(conf, jobInterMediate[1]+"/part-r-00000", args[2], args[3]);
	    	}
//	    }
		
//	    FileSystem fs = FileSystem.get(conf);
//		Path tmp = new Path(jobInterMediate);
//	    fs.delete(tmp, true);
	    System.exit(ret? 0 : 1);
	}

}
