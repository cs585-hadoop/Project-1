package org.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query4Driver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Usage: Query-4 <HDFS input file> <HDFS output file> <HDFS cache file>");
			System.exit(2);
		}
		conf.set("mapred.textoutputformat.separator", ",");
		// TODO:add cache files
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);
		
		Job job = Job.getInstance(conf, "Query-4");
		job.setJarByClass(org.apache.hadoop.Query4Driver.class);
		
		// TODO: specify a mapper
		job.setMapperClass(org.apache.hadoop.Query4Mapper.class);
		// TODO: specify a reducer
		job.setReducerClass(org.apache.hadoop.Query4Reducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (!job.waitForCompletion(true))
			return;
	}

}
