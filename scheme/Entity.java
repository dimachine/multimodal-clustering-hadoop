package ru.hse.tochilkin.multiclustering.scheme;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Entity implements Writable{
	private String value;
	private int index;
	
	public Entity() {
		index = -1;
		value = "";
	}
	
	public Entity(int index, String value) {
		this.index = index;
		this.value = value;
	}
	
	public int getIndex() {
		return index;
	}
	
	public String getValue() {
		return value;
	}
	
	public void set(int ind, String val) {
		index = ind;
		value = val;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		IntWritable ind = new IntWritable();
		ind.readFields(in);
        index = ind.get();
        Text val = new Text();
        val.readFields(in);
        value = val.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		new IntWritable(index).write(out);
		new Text(value).write(out);
	}
	
	@Override
	public String toString() {
		return value;
	}

}
