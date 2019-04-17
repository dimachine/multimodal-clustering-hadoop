package ru.hse.tochilkin.multiclustering.scheme;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.sun.org.apache.xml.internal.serializer.ToTextSAXHandler;

public class Cumulus implements Writable, Iterable<String>, Comparable<Cumulus>{
	private int index;
	private String[] values;
	
	public Cumulus() {
		index = -1;
		values = new String[0];
	}
	
	public Cumulus(int index, String[] vals) {
		Arrays.sort(vals);
		values = vals;
		this.index = index;
	}
	
	public Cumulus(Text text) {
		String[] data = text.toString().split(",");
		index = Integer.parseInt(data[0]);
		values = new String[data.length - 1];
		for (int i = 0; i < data.length - 1; ++i) {
			values[i] = data[i + 1]; 
		}
	}
	
	public int getIndex() {
		return index;
	}
	
	public String[] getValues() {
		return values;
	}
	
	@Override
    public void write(DataOutput output) throws IOException {
		toText().write(output);
	}
	
	@Override
    public void readFields(DataInput input) throws IOException {
		Text text = new Text();
		text.readFields(input);
		Cumulus delate = new Cumulus(text);
		values = delate.values;
		index = delate.index;
	}

	@Override
	public String toString(){
		return "{" + getRecord() + "}";
	}
	
	public Text toText() {
		return new Text(index + "," + getRecord());
	}
	
	public String getRecord() {
		if (values.length == 0) {
			return "";
		}
		StringBuilder stringBuilder = new StringBuilder().append(values[0]);
		for (int i = 1; i < values.length; ++i) {
			stringBuilder.append("," + values[i]);
		}
		
		return stringBuilder.toString();
	}

	@Override
	public Iterator<String> iterator() {
		return new EntitySetIterator(values);
	}

	@Override
	public int compareTo(Cumulus o) {
		if (index < o.index)
			return -1;
		if (index > o.index)
			return 1;
		return 0;
	}

}

class EntitySetIterator  implements Iterator<String> {
	String[] entities;
	int index;
	
	public EntitySetIterator(String[] entities) {
		this.entities = entities;
		index = 0;
	}
	
	@Override
	public boolean hasNext() {
		return (index < entities.length);
	}

	@Override
	public String next() {
		return entities[index++];
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub		
	}
	
}
