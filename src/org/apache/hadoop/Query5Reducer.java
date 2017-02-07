package org.apache.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Query5Reducer extends Reducer<Text, Text, Text, Text> {

	private static HashMap<String, Integer> cusRecord = new HashMap<String, Integer>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int transcount = 0;

		for (Text val : values) {
			transcount += 1;
		}
		cusRecord.put(key.toString(), transcount);
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		int total = 0;
		Set<String> set = cusRecord.keySet();
		
		for (String s : set) {
			total+=cusRecord.get(s);
		}
		float avg = total/cusRecord.size();
		
		for (String s : set) {
			if(cusRecord.get(s)>=avg)
				context.write(new Text(s), null);
		}
	}
}
