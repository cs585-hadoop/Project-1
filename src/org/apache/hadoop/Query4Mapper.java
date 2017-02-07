package org.apache.hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Query4Mapper extends Mapper<LongWritable, Text, Text, Text> {

	private static HashMap<String,String> customermap=new HashMap<String,String>();
	private static BufferedReader reader;
	
	//getting the path of the file to be loaded to hashmap
	@Override
	protected void setup(Context context) throws IOException,InterruptedException,FileNotFoundException{
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		System.err.println(cacheFilesLocal);
		for (Path eachPath : cacheFilesLocal) {
			
			if (eachPath.getName().toString().trim().equals("customer.csv")) {
				loadCustomerHashMap(eachPath, context);
			}
		}
	}	
	
	private void loadCustomerHashMap(Path p, Context context) throws IOException,FileNotFoundException{
		reader=new BufferedReader(new FileReader(p.toString()));
		String inputline=reader.readLine();
		String[] input;
		while(inputline!=null){
			input=inputline.split(",");
			//loading the country code of each customer
			customermap.put(input[0],input[3]);	
			inputline=reader.readLine();
		}
	}
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		
		StringTokenizer itr=new StringTokenizer(ivalue.toString());
		String[] tokens=null;
		String line;
		
		while(itr.hasMoreTokens()){
			line=itr.nextToken();
			tokens=line.split(",");
			context.write(new Text(customermap.get(tokens[1])),new Text(line));
		}
	}
}
