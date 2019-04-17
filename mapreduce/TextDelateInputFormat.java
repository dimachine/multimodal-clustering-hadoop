package ru.hse.tochilkin.multiclustering.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import ru.hse.tochilkin.multiclustering.scheme.Cumulus;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;

public class TextDelateInputFormat implements RecordReader<Text, Cumulus> {
	private LineRecordReader lineReader;
	private LongWritable lineKey;
	private Text lineValue;
	  
	@Override
	public boolean next(Text key, Cumulus value) throws IOException {
		// get the next line
	    if (!lineReader.next(lineKey, lineValue)) {
	      return false;
	    }

	    return true;
	}
	
	@Override
	public Text createKey() {
		return new Text("");
	}
	
	@Override
	public Cumulus createValue() {
		return new Cumulus();
	}
	
	@Override
	public long getPos() throws IOException {
		return lineReader.getPos();
	}
	
	@Override
	public void close() throws IOException {
		lineReader.close();
	}
	
	@Override
	public float getProgress() throws IOException {
		return lineReader.getProgress();
	}
	  
	  
}
