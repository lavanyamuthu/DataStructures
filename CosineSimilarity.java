import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

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
	private static String query = "";
	public static HashMap<String,Integer> QueryWordsCount = new HashMap<String, Integer>();
	public static HashMap<String,Double> Querytf = new HashMap<String, Double>();
	public static HashMap<String,Double> Queryidf = new HashMap<String, Double>();
	//public static HashMap<String,querytfidfdata> Querytfidf = new HashMap<String, querytfidfdata>();
	public static HashMap<String,Double> Querytfidf = new HashMap<String, Double>();
	public static HashMap<String,Double> Doctf = new HashMap<String, Double>();
	public static HashMap<String,doctfidfdata> Wordtfidf = new HashMap<String, doctfidfdata>();
	public static HashMap<String,HashMap<String,doctfidfdata>> Doctfidf = new HashMap<String, HashMap<String,doctfidfdata>>();
	public static ArrayList<String> Docid = new ArrayList<String>();
	public static class CosineMapper extends Mapper<LongWritable,Text,Text,Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (query.equals("")) {
				Configuration conf = context.getConfiguration();
				query = new String(conf.get("query"));
			}
			query = query.toLowerCase();
			String [] token = query.split(" ");
			Text word = new Text();
			Text location = new Text();
			//String[] parse = key.toString().split("\t");
			String temp= "";
			String[] tokenize = value.toString().split("@");
			String[] parse = tokenize[1].split(",");
			
			//System.out.println("key:"+key+" value:"+value+" token length:"+token.length);
			for(int i=0;i<token.length;i++) {
				//System.out.println("In for loop, tokenize[0]:"+tokenize[0]+" token[i]:"+token[i]);
				if(tokenize[0].trim().equals(token[i].trim())) {
					word.set(tokenize[0]);
					location.set(tokenize[1]);
					//System.out.println("word:"+word+" location:"+location);
					context.write(word,location);
					if(!Queryidf.containsKey(token[i].trim())) {
						Queryidf.put(token[i].trim(),Double.parseDouble(parse[2]));
					}
					/*if(!Docid.contains(token[i].trim())) {
						Docid.add(token[i].trim());
					}*/
					//else {
					//	Queryidf.put(token[i].trim(),Queryidf.get(key.toString()) + 1);
					//}
				}
			}
			//word.set("key");
			//temp = parse[1]+":"+value;
			//location.set(value);
			//context.write(word, location);
			//System.out.println("word:"+word+" location:"+location);
			/*if(!treeMap.containsKey(key.toString())) {
				treeMap.put(key.toString(),1);
			}
			else {
				treeMap.put(key.toString(), treeMap.get(key.toString()) + 1);
			}*/
		}	
	}
	
	public static class CosineReducer extends Reducer<Text,Text,Text,Text> {
		static String[] gileList = null;
		//private static String query = "";
	    static List<DataForRank> rList = new ArrayList<DataForRank>();
	    public static double cosineSimilarity(double[] docVector1,
				double[] docVector2) {
			double dotProduct = 0.0;
			double magnitude1 = 0.0;
			double magnitude2 = 0.0;
			double cosineSimilarity = 0.0;

			for (int i = 0; i < docVector1.length; i++) {
				dotProduct += docVector1[i] * docVector2[i];
				magnitude1 += Math.pow(docVector1[i], 2);
				magnitude2 += Math.pow(docVector2[i], 2);
			}

			magnitude1 = Math.sqrt(magnitude1);
			magnitude2 = Math.sqrt(magnitude2);

			if (magnitude1 != 0.0 | magnitude2 != 0.0) {
				cosineSimilarity = dotProduct / (magnitude1 * magnitude2);
			} else {
				return 0.0;
			}
			return cosineSimilarity;
		}


		public static void getCosineSimilarity(Context context) throws IOException, InterruptedException {
			double[] vec1 = new double[Doctfidf.size()];
			double[] vec2 = new double[Doctfidf.size()];

			/*for (Map.Entry<String, HashMap<String, doctfidfdata>> entry : Doctfidf.entrySet()) {
				HashMap<String, doctfidfdata> temp1 = entry.getValue();
				//context.write(new Text(entry.getKey()),new Text("It is the doc id"));
				for (Map.Entry<String, doctfidfdata> entry1 : temp1.entrySet()) {
					Text test = new Text(entry1.getKey()+entry1.getValue().tfidf+","+entry1.getValue().querytfidf);
					//context.write(new Text(entry.getKey()),test);
					System.out.println("Key = " + entry.getKey() + ", test = " + test);
				}
			}*/
			
			String name = "";
			if (query.equals("")) {
				Configuration conf = context.getConfiguration();
				query = new String(conf.get("query"));
			}
			System.out.println(query);
			query = query.toLowerCase();
			String [] token = query.split(" ");
			StringTokenizer tokens1 = new StringTokenizer(query);
			String token1 = "";
			Set<String> files = null;
			int index = token.length;
			for(int i=0;i<token.length;i++) {
				if(Doctfidf.get(token[i])!=null){
					files = Doctfidf.get(token[i]).keySet();
					for(String file:files){
						gileList[index]=file;
						index++;
						//System.out.println("gileList[index]:"+gileList[index]);
					}
				}
			}
			/*while (tokens1.hasMoreTokens()) {
				token1 = tokens1.nextToken();
				if(Doctfidf.get(token1)!=null){
					files = Doctfidf.get(token1).keySet();
					for(String file:files){
						gileList[index]=file;
						index++;
					}
				}
			}*/
			//System.out.println("index:"+index);
			for (int i = 0; i < index; i++) {
				//name = gileList[i];
				name = Docid.get(i);
				System.out.println("Docid.get(i):"+Docid.get(i));
				StringTokenizer tokens = new StringTokenizer(query);
				String token2 = "";
				int j = 0;
				for(int k=0;k<token.length;k++) {
					//System.out.println("token[k]:"+token[k]+" name is:"+name);
					if (Doctfidf.get(name) != null) {
						//System.out.println("name:"+name+" Doctfidf.get(name):"+Doctfidf.get(name));
						//System.out.println("1st loop Doctfidf.get(name).get(token[k]):"+Doctfidf.get(name).get(token[k].trim()));
						//vec1[j] = Doctfidf.get(name).get(token[k].trim()).gettfidf();
						//vec2[j] = Doctfidf.get(name).get(token[k].trim()).getquerytfidf();
						HashMap<String, doctfidfdata> temp1 = new HashMap<String, doctfidfdata>();
						//HashMap<String, doctfidfdata> temp1 = find(name, Doctfidf);
						//if(	Doctfidf.get(name).get(token[k].trim()) != null) {
						temp1 = Doctfidf.get(name);
						if(!temp1.isEmpty()) {
							//System.out.println("temp1 is not empty");
							//for (Map.Entry<String, doctfidfdata> entry1 : temp1.entrySet()) {
							//	System.out.println("key:"+entry1.getKey()+" value:"+entry1.getValue().getquerytfidf());
							//}
							//System.out.println("token[k]:"+token[k]+" temp val:"+temp1.get(token[k].trim()));
							if(temp1.containsKey(token[k].trim())) { 
								//System.out.println("1st loop Doctfidf.get(name).get(token[k]):"+temp1.get(token[k].trim()));
								vec1[j] = temp1.get(token[k].trim()).gettfidf();
								vec2[j] = temp1.get(token[k].trim()).getquerytfidf();
							}
						}
					}
					else{
						vec1[j] = 0.0;
						/*Set<String> tmpTfIdf = Doctfidf.get(name).keySet();
						String docName = "";
						for (String tmpName : tmpTfIdf) {
							docName = tmpName;
							break;
						}*/
						vec2[j] = 0.0;
						if(Doctfidf.get(name) != null) {
							//System.out.println("2nd loop Doctfidf.get(name):"+Doctfidf.get(name)+" token:"+token);
							if (Doctfidf.get(name).get(token[k].trim()) != null) {
								//System.out.println("Doctfidf.get(name).get(token[k]):"+Doctfidf.get(name).get(token[k].trim()));
								vec2[j] = Doctfidf.get(name).get(token[k].trim()).getquerytfidf();
							}
						}
					}
					//System.out.println("vec1[j]:"+vec1[j]+" vec2[j]:"+vec2[j]);
					j++;
				}
				/*while (tokens.hasMoreTokens()) {
					
					token2 = tokens.nextToken();
					System.out.println("token:"+token2);
					if (Doctfidf.get(name).get(token2) != null) {
						vec1[j] = Doctfidf.get(name).get(token2).tfidf;
						vec2[j] = Doctfidf.get(name).get(token2).querytfidf;
						
					}
					else{
						vec1[j] = 0.0;
						vec2[j] = Doctfidf.get(name).get(token2).querytfidf;
					}
					System.out.println("vec1[j]:"+vec1[j]+" vec2[j]:"+vec2[j]);
					j++;
				}*/
				
			//	context.write(new Text(name), new Text(new Double(cosineSimilarity(vec1,vec2)).toString()));
					rList.add(new DataForRank(name, cosineSimilarity(vec1,
							vec2)));

			}

		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (query.equals("")) {
				Configuration conf = context.getConfiguration();
				query = new String(conf.get("query"));
			}
			query = query.toLowerCase();
			String [] token = query.split(" ");
			for(int i=0;i<token.length;i++) {
				if(!QueryWordsCount.containsKey(token[i])) {
					QueryWordsCount.put(token[i],1);
				}
				else {
					QueryWordsCount.put(token[i], QueryWordsCount.get(token[i] + 1));
				}
			}
			//Iterator it = QueryWordsCount.entrySet().iterator();
			for (Map.Entry<String, Integer> entry : QueryWordsCount.entrySet()) {
			    //System.out.println("wcKey = " + entry.getKey() + ", Value = " + entry.getValue()+" totla length:"+token.length);
			    if(!Querytf.containsKey(entry.getKey())) {
					Querytf.put(entry.getKey(),(double) ((double)entry.getValue()/(double)token.length));
					//Querytf.put(entry.getKey(),(double) );
				}
			}
			for(int i=0;i<token.length;i++) {
				//querytfidfdata querydata = new querytfidfdata(token[i],(Querytf.get(token[i])*Queryidf.get(token[i])));
				//querytfidfdata querydata = new querytfidfdata();
				if(!Querytfidf.containsKey(token[i])) {
					//querydata.word = token[i];
					//querydata.tfidf = Querytf.get(token[i])*Queryidf.get(token[i]);
					Querytfidf.put(token[i].trim(),Querytf.get(token[i])*Queryidf.get(token[i]));
				}
				//else {
				//	Querytfidf.put(token[i], Querytfidf.get(token[i] + 1);
				//}
			}
			for (Map.Entry<String, Double> entry : Querytf.entrySet()) {
			   // System.out.println("tfKey = " + entry.getKey() + ", Value = " + entry.getValue());
			    if(!Querytf.containsKey(entry.getKey())) {
					Querytf.put(entry.getKey(),(double) (entry.getValue()/token.length));
				}
			}
			Iterator itr = values.iterator();
			String tokenize = " ", reducedval = "";
			//String[] parse = new String[2];
			double idf=0;
			double tf=0;
			int n=0;
			while (itr.hasNext()) {
				tokenize = itr.next().toString();
				String[] parse = tokenize.split(":");
				String[] parseval = parse[3].split(",");
				//System.out.println("key:"+key+" token:"+tokenize);
				//doctfidfdata docdata = new doctfidfdata(key.toString(),Double.parseDouble(parseval[3]));
				if(!Doctfidf.containsKey(parse[0])) {
					Docid.add(parse[0].trim());
					//System.out.println("docid:"+parse[0].trim()+" word:"+key.toString()+", docid:"+parse[0]+", tfidf:"+Double.parseDouble(parseval[3])+" querytfidf:"+Querytfidf.get(key.toString().trim()));
					HashMap<String,doctfidfdata> tfidfmap = new HashMap<String,doctfidfdata>();
					doctfidfdata temp = new doctfidfdata(Double.parseDouble(parseval[3]),Querytfidf.get(key.toString().trim()));
					tfidfmap.put(key.toString().trim(), temp);
					Doctfidf.put(parse[0].trim(), tfidfmap);	
				}
				else {
					//System.out.println("secnddocid:"+parse[0].trim()+" word:"+key.toString()+", docid:"+parse[0]+", tfidf:"+Double.parseDouble(parseval[3])+" querytfidf:"+Querytfidf.get(key.toString().trim()));
					HashMap<String,doctfidfdata> tfidfmap = new HashMap<String,doctfidfdata>();
					doctfidfdata temp = new doctfidfdata(Double.parseDouble(parseval[3]),Querytfidf.get(key.toString().trim()));
					//tfidfmap.put(key.toString(), temp);
					Doctfidf.get(parse[0].trim()).put(key.toString().trim(), temp);
					//, tfidfmap);
				}
			}
			//TreeMap<String,HashMap<String,doctfidfdata>> Doctfidf1 = new TreeMap<String, HashMap<String,doctfidfdata>>(Doctfidf);
						
			//Collections.sort(Doctfidf);
			
			/*for (Map.Entry<String, HashMap<String, doctfidfdata>> entry : Doctfidf.entrySet()) {
				HashMap<String, doctfidfdata> temp1 = entry.getValue();
				//context.write(new Text(entry.getKey()),new Text("It is the doc id"));
				for (Map.Entry<String, doctfidfdata> entry1 : temp1.entrySet()) {
					Text test = new Text(entry1.getKey()+entry1.getValue().tfidf+","+entry1.getValue().querytfidf);
					context.write(new Text(entry.getKey()),test);
				}
			    //System.out.println("Key = " + entry.getKey() + ", word = " + entry.getValue());
			    
			}*/
			/*for(int i=0;i<Docid.size();i++) {
				System.out.println("DocId:"+Docid.get(i)+" key:"+Doctfidf.get(Docid.get(i)).size());
			}*/
			getCosineSimilarity(context);
			Collections.sort(rList);
			for(DataForRank doc:rList){
			context.write(new Text(doc.toString().split("::::::::::::")[0]), new Text(doc.toString().split("::::::::::::")[1]));
			}
			//System.out.println("Doctfidf size:"+Doctfidf.size()+" size of 2508 docid:"+Doctfidf.get("2508").size());
			/*for (Map.Entry<String, Double> entry : Querytfidf.entrySet()) {
				Text test = new Text(entry.getValue().word+entry.getValue().tfidf);
			    System.out.println("Key = " + entry.getKey() + ", word = " + entry.getValue().word+", tfidf = "+entry.getValue().tfidf);
			    context.write(new Text(entry.getKey()),test);
			}*/
		}
	}

	private static final String OUTPUT_PATH = "data//output//queryop";

	// where to read the data from.
	private static final String INPUT_PATH = "data//output//RR//stage3//part-r-00000";

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
		//System.out.println("params length:"+params.length);
		//System.out.println(params[0]+params[1]+params[2]);
		int res = ToolRunner.run(new Configuration(), new CosineSimilarity(), params);
		//int res = ToolRunner.run(new SearchDist(), params);
	    //List<DataForRank> rList = new ArrayList<DataForRank>();
		
		System.exit(res);
	}
}
class doctfidfdata {
	//String word;
	//String docid;
	//int d;
	//int D;
	//int n;
	//int tf;
	//int idf;
	double tfidf;
	double querytfidf;
	doctfidfdata(double tfidf, double querytfidf) {
		this.tfidf = tfidf;
		//this.docid = docid;
		this.querytfidf = querytfidf;
	}
	public double gettfidf() {
		return this.tfidf;
	}
	public double getquerytfidf() {
		return this.querytfidf;
	}
}
class querytfidfdata {
	String word;
	//int docid;
	//int d;
	//int D;
	//int n;
	//int tf;
	//int idf;
	double tfidf;
	querytfidfdata(String word, double tfidf) {
		this.word = word;
		this.tfidf = tfidf;
	}
}
class DataForRank implements Comparable<DataForRank> {
	String docName;
	double tfidf;
	DataForRank(String doc, double tfidf){
		this.docName = doc;
		this.tfidf = tfidf;
	}
	public int compareTo(DataForRank rd) {
		int ret = 0;
		if (rd instanceof DataForRank) {
			if (this.tfidf == ((DataForRank) rd).tfidf) {
				ret = 0;
			} else
				ret = this.tfidf < ((DataForRank) rd).tfidf ? 1 : -1;
		}
		return ret;
	}
	public String toString(){
		return "Similarity = "+tfidf + "::::::::::::Doc= "+docName;
	}

}