import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RRStage2NumberofWordsinDoc extends Configured implements Tool{
	public static Map<String, Integer> treeMap = new TreeMap<String, Integer>();
	public static class RRStage2Mapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
			Text word = new Text();
			Text location = new Text();
			String[] parse = key.toString().split(":");
			String temp= "";
			word.set(parse[0]);
			temp = parse[1]+":"+value;
			location.set(temp);
			context.write(word, location);
			//System.out.println("word:"+word+" location:"+location);
			if(!treeMap.containsKey(parse[1])) {
				treeMap.put(parse[1],Integer.parseInt(value.toString()));
			}
			else {
				treeMap.put(parse[1], treeMap.get(parse[1]) + Integer.parseInt(value.toString()));
			}
		}
	}
	public static class RRStage2Reducer1 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
			/*for(Map.Entry<String,Integer> entry : treeMap.entrySet()) {
				Integer value = entry.getValue();
				System.out.println(entry.getKey() + " => " + value);
			}*/
			Iterator itr = values.iterator();
			String token = " ", reducedval = "";
			String[] parse = new String[2];
			int count=0;
			while (itr.hasNext()) {
				//System.out.println("key:"+key+" token:"+token);
				token = itr.next().toString();
				parse = token.toString().split(":");
				//count = count+Integer.parseInt(parse[1]);
				reducedval = token+":"+treeMap.get(parse[0]);
				context.write(key, new Text(reducedval));
			}
			//System.out.println("o/p key:"+parse[0]+" content:"+key+":"+count);
			//System.out.println("key:"+key);
			//context.write(key, new Text("test"));
			
		}
	}
	private static String input = "data//output//RR//stage1//part-r-00000";
	private static String output = "data//output//RR//stage2";
	public final int run(String[] args) throws Exception { // NOPMD 
		  Configuration conf = getConf(); 
		  Job job = new Job(conf); 
		  job.setJobName("RRStage2NumberofWordsinDoc");  
		  job.setMapperClass(RRStage2Mapper.class); 
		  job.setReducerClass(RRStage2Reducer1.class); 
		  //job.setReducerClass(RRStage2Reducer2.class);
		  //job.setInputFormatClass(TextInputFormat.class);
		  job.setInputFormatClass(KeyValueTextInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		  job.setOutputKeyClass(Text.class); 
		  job.setOutputValueClass(Text.class);
		  FileInputFormat.addInputPath(job, new Path(input)); 
		  FileOutputFormat.setOutputPath(job, new Path(output));
		  //FileInputFormat.addInputPath(job, new Path(args[0])); 
		  //FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1); 
		  return 0; 
	} 
	public static void main(String[] args) {
		int res = 0;
		try {
			res = ToolRunner.run(new RRStage2NumberofWordsinDoc(), args);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		System.exit(res); 
	}
}