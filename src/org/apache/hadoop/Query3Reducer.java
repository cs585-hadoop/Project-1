package org.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Query3Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		float totaltrans=0;
		int minterms=99;
		int count=0;
		int current_minterms;
		String[] fields = null;
		for (Text val : values) {
			count++;
			fields=val.toString().split(",");
			totaltrans+=Float.parseFloat(fields[4]);
			current_minterms=Integer.parseInt(fields[5]);
			if(current_minterms<minterms){
				minterms=current_minterms;
			}
		}
		String name=fields[0];
		String salary=fields[1];
		context.write(key,new Text(name+","+salary+","+count+","+totaltrans+","+minterms));
	}

}
