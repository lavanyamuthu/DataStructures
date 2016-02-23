import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UniwordIndex extends Configured implements Tool{
	public static class UniwordIndexMapper extends Mapper<Text, BytesWritable, Text, Text>{
		public void map(Text key, BytesWritable content, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			Text location = new Text();
			ReutersDoc FileContent = StringUtils.getXMLContent(content);
			//System.out.println("key is:"+key);
			location.set(FileContent.getDocID());
			String extractedContents = StringUtils.normalizeText(FileContent.getContent());
			//System.out.println("After normalization: "+extractedContents);
			String[] temp = extractedContents.split(" ");
			for(int i=0;i<temp.length;i++) {
				if(!temp[i].equals(" ") && !temp[i].equals("a") && !temp[i].equals("aa") && !temp[i].equals("aaa") 
					&& !temp[i].equals("") && !temp[i].equals("aaaaaa") && !temp[i].equals("aaabaa") && !temp[i].equals("aaabbbplus") 
					&& !temp[i].equals("aaafaaa") && !"".equals(temp[i].trim()) && !temp[i].equals("of")) {
					word.set(temp[i]);
					context.write(word, location);
				}
			}
		}
	}
	public static class UniwordIndexReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> location, Context context) throws IOException, InterruptedException {
			ArrayList<String> a2 = new ArrayList<String>();
			Iterator itr = location.iterator();
			String token = "";
			StringBuilder sb = new StringBuilder();
			while (itr.hasNext()) {
			token = itr.next().toString();
				if(!a2.contains(token))
				{
					sb.append(token);
				    sb.append(",");
					a2.add(token);
				}
			}
			Collections.sort(a2);
			String temp = a2.size()+" : "+sb.substring(0,sb.length()-1);
			context.write(key,new Text(temp));
			//}
		}
	}
	
	private static String input = "data//txt1";
	private static String output = "data//output//uniword";
	public final int run(String[] args) throws Exception { // NOPMD 
		  Configuration conf = getConf(); 
		  Job job = new Job(conf); 
		  job.setJobName("UniwordIndex");  
		  job.setMapperClass(UniwordIndexMapper.class); 
		  job.setReducerClass(UniwordIndexReducer.class); 
		  job.setInputFormatClass(ZipFileInputFormat.class);
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
			res = ToolRunner.run(new UniwordIndex(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		System.exit(res); 
	}
}