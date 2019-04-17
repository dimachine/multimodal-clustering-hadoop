package ru.hse.tochilkin.multiclustering.app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ru.hse.tochilkin.multiclustering.exceptions.WrongDelimiterFormatException;
import ru.hse.tochilkin.multiclustering.mapreduce.FirstMapper;
import ru.hse.tochilkin.multiclustering.mapreduce.FirstReducer;
import ru.hse.tochilkin.multiclustering.mapreduce.SecondMapper;
import ru.hse.tochilkin.multiclustering.mapreduce.SecondReducer;
import ru.hse.tochilkin.multiclustering.mapreduce.ThirdMapper;
import ru.hse.tochilkin.multiclustering.mapreduce.ThirdReducer;
import ru.hse.tochilkin.multiclustering.params.Constants;
import ru.hse.tochilkin.multiclustering.scheme.Entity;
import ru.hse.tochilkin.multiclustering.scheme.Cumulus;

public class App 
{
	public static void main(String[] args) throws IOException,
    InterruptedException, ClassNotFoundException {
		Path inputPath, outputDir;
		Integer modality = null;
		try {
			if (args.length < 3) {
				throw new IllegalArgumentException("Number of passed arguments is too small");
			}
			modality = Integer.parseInt(args[2]);
			if (modality < 2) {
				throw new IllegalArgumentException("Modality should be more than 2");
			}
		}
		catch (NumberFormatException e) {
			e.printStackTrace();
			return;
		}
		catch (IllegalArgumentException e) {
			e.printStackTrace();
			return;
		}
		inputPath = new Path(args[0]);
		outputDir = new Path(args[1]);
		
		String entityDelimeter = (args.length > 3) ? args[3] : Constants.defaultEntityDelimeter;		
		try {
			if (entityDelimeter.isEmpty()) {
				throw new WrongDelimiterFormatException(
						"Delimeter must contain at least one character");
			}
		}
		catch (WrongDelimiterFormatException e) {
			e.printStackTrace();
			return;
		}
		
		String recordsDelimeter = (args.length > 4) ? args[4] : Constants.defaultRecordDelimeter;		
		try {
			if (recordsDelimeter.isEmpty()) {
				throw new WrongDelimiterFormatException(
						"Delimeter must contain at least one character");
			}
		}
		catch (WrongDelimiterFormatException e) {
			e.printStackTrace();
			return;
		}

		
		Path firstJobOutput = new Path(Constants.firstJobOutput);
		Path secondJobOutput = new Path(Constants.secondJobOutput);

		long startTime = System.currentTimeMillis();
		Configuration conf1 = new Configuration();
		//conf1.set("textinputformat.record.delimiter", "/n");
		conf1.set(Constants.entityDelimeterParameter, entityDelimeter);
		conf1.setInt(Constants.modalityParameter, modality);
		//conf1.setInt("num_keys", 4);
		Job job1 = JobConfigurator.getJob(conf1, Constants.firstStageName, 1, 
				FirstMapper.class, FirstReducer.class, 
				Text.class, Entity.class, 
				Text.class, Text.class, 
				inputPath, TextInputFormat.class, 
				firstJobOutput, TextOutputFormat.class);		
		
		// Execute job
		int code = job1.waitForCompletion(true) ? 0 : 1;		
		if (code == 1) {
			System.exit(code);
			return;
		}
		long intermediate = System.currentTimeMillis();
		System.out.println("Elapsed time for the first stage: " + 
				new Long(intermediate - startTime) + "ms\n\n");
		
		Configuration conf2 = new Configuration();
		conf2.set(Constants.numKeysParameter, "");
		conf2.set(Constants.modalityParameter, "");
		Job job2 = JobConfigurator.getJob(conf2, Constants.secondStageName, 1, 
				SecondMapper.class, SecondReducer.class, 
				Text.class, Cumulus.class, 
				Text.class, Text.class, 
				firstJobOutput, KeyValueTextInputFormat.class, 
				secondJobOutput, TextOutputFormat.class);		
		
		code = job2.waitForCompletion(true) ? 0 : 1;
		if (code == 1) {
			System.exit(code);
			return;
		}
		
		System.out.println("Elapsed time for the second stage: " + 
				new Long(System.currentTimeMillis() - intermediate) + "ms\n\n");
		intermediate = System.currentTimeMillis();
		
		Configuration conf3 = new Configuration();
		conf3.set(Constants.numKeysParameter, "");
		conf3.set(Constants.modalityParameter, "");
		Job job3 = JobConfigurator.getJob(conf3, Constants.thirdStageName, 1, 
				ThirdMapper.class, ThirdReducer.class, 
				Text.class, Text.class, 
				Text.class, Text.class, 
				secondJobOutput, KeyValueTextInputFormat.class, 
				outputDir, TextOutputFormat.class);		
		
		code = job3.waitForCompletion(true) ? 0 : 1;
		System.out.println("Elapsed time for the third stage: " + 
				new Long(System.currentTimeMillis() - intermediate) + "ms");
		System.out.println("TOTAL Elapsed time: " + 
				new Long(System.currentTimeMillis() - startTime) + "ms");
		
		System.exit(code);
	}
	
	
	
}
