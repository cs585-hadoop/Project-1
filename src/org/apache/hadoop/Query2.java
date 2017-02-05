package org.apache.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query2 {
	public static class Q2Mapper extends Mapper<Object, Text, Text, Text> {
		private Text cusID = new Text();
		private Text record = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String[] s = itr.nextToken().split(",");
				cusID.set(s[1]);
				record.set(s[2] + ",1");
				context.write(cusID, record);
			}
		}
	}

	public static class Q2Combiner extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
			float total = 0;
			int count = 0;
			String[] s = value.toString().split(",");
			total += Float.parseFloat(s[0]);
			count += Integer.parseInt(s[1]);
			result.set(total + "," + count);
			context.write(key, result);
		}
	}

	public static class Q2Reducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			float total = 0;
			int count = 0;
			String[] s = null;
			for (Text val : values) {
				s = val.toString().split(",");
				total += Float.parseFloat(s[0]);
				count += Integer.parseInt(s[1]);
			}
			result.set(total + "," + count);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: Query-2 <HDFS input file> <HDFS output file>");
			System.exit(2);
		}
		Job job = new Job(conf, "Query-2");
		job.setJarByClass(Query2.class);
		job.setMapperClass(Q2Mapper.class);
		job.setCombinerClass(Q2Combiner.class);
		job.setReducerClass(Q2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(2);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
