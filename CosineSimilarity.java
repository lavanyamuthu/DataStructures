import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CosineSimilarity extends Configured implements Tool {
	public static class CosineMapper extends Mapper<LongWritable,Text,Text,Text> {
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		
		}	
	}
	
	public static class CosineReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		}
	}

	private static final String OUTPUT_PATH = "data//queryop";

	// where to read the data from.
	private static final String INPUT_PATH = "data//inp2";

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		conf.set("query", args[2]);
		Job job = new Job(conf, "Final Phase");

		job.setMapperClass(CosineMapper.class);
		job.setReducerClass(CosineReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

		
		  //FileInputFormat.addInputPath(job, new Path(args[0]));
		  //FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		job.setJarByClass(CosineSimilarity.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String [] params = new String[3];
		int i =0;
		for(i=0;i<args.length;i++){
			params[i]=args[i];
		}
		FileReader file = new FileReader("data//extract//Query.txt");

		//Configuration conf = new Configuration(); 
		
		BufferedReader br1 = new BufferedReader(file);
		
		params[i] = br1.readLine();
		System.out.println("params lenght:"+params.length);
		System.out.println(params[0]+params[1]+params[2]);
		int res = ToolRunner.run(new Configuration(), new CosineSimilarity(), params);
		//int res = ToolRunner.run(new SearchDist(), params);
	    //List<DataForRank> rList = new ArrayList<DataForRank>();
		
		System.exit(res);
	}

}