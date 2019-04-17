package ru.hse.tochilkin.multiclustering.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ru.hse.tochilkin.multiclustering.scheme.Cumulus;
import ru.hse.tochilkin.multiclustering.scheme.Tuple;

public class SecondMapper 
		extends Mapper<Text, Text, Text, Cumulus>{
	
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		Cumulus delate = new Cumulus(value);
		int index = delate.getIndex();
		Tuple relation = new Tuple(key);
		
		Iterator<String> setIterator = delate.iterator();
		while (setIterator.hasNext()) {
			relation.setElement(index, setIterator.next());
			context.write(relation.toText(), delate);
		}
	}
}
