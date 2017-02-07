package org.apache.hadoop;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Query4Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//Hashset to store the customers for a country code
		HashSet<String> customerset=new HashSet<String>();
		
		//key:Country Value:Record
		float maxTranstotal=0;
		float minTranstotal=9999;
		
		String[] tokens=null;
		String cusid;
		float transtotal=0;
		int cuscount=0;
		
		for (Text val : values) {
			tokens=val.toString().split(",");
			cusid=tokens[1];
			transtotal=Float.parseFloat(tokens[2]);
			if(!customerset.contains(cusid)){
				customerset.add(cusid);
			}
			if(transtotal>maxTranstotal){
				maxTranstotal=transtotal;
			}
			if(transtotal<minTranstotal){
				minTranstotal=transtotal;
			}
		}
		cuscount=customerset.size();
		context.write(key,new Text(cuscount+","+maxTranstotal+","+minTranstotal));
	}

}
