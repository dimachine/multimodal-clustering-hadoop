package ru.hse.tochilkin.multiclustering.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobConfigurator {
	public static Job getJob (Configuration conf, String name, int reducerNumber,
			Class<? extends Mapper> mapper, Class<? extends Reducer> reducer,
			Class<?> mapperOutputKey, Class<?> mapperOutputValue,
			Class<?> reducerOutputKey, Class<?> reducerOutputValue,
			Path inputPath, Class<? extends InputFormat> inputFormat,
			Path outputPath, Class<? extends OutputFormat> outputFormat) throws IOException {
		
		// Create job
		Job job = new Job(conf, "Multiclustering");
		job.setJarByClass(mapper);
		
		// Setup MapReduce
		job.setMapperClass(mapper);
		job.setReducerClass(reducer);
		job.setNumReduceTasks(reducerNumber);
		
		// Specify key / value
		job.setMapOutputKeyClass(mapperOutputKey);
		job.setMapOutputValueClass(mapperOutputValue);
		
		job.setOutputKeyClass(reducerOutputKey);
		job.setOutputValueClass(reducerOutputValue);
		
		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(inputFormat);
		
		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputPath))
		    hdfs.delete(outputPath, true);
		// Output
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(outputFormat);
		
		return job;				
	}
}
