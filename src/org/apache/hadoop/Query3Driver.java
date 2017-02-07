package org.apache.hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import query.Query3Mapper;
import query.Query3Reducer;

public class Query3Driver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		if (args.length != 3) {
			System.err.println("Usage: Query-3 <HDFS input file> <HDFS output file> <HDFS cache file>");
			System.exit(2);
		}
		
		// TODO:add cache files
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);
				
		Job job = Job.getInstance(conf, "Query-3");
		job.setJarByClass(org.apache.hadoop.Query3Driver.class);
		
		// TODO: specify a mapper
		job.setMapperClass(Query3Mapper.class);
		// TODO: specify a reducer
		job.setReducerClass(Query3Reducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//job.setNumReduceTasks(0);


		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		

		if (!job.waitForCompletion(true))
			return;
	}

}
