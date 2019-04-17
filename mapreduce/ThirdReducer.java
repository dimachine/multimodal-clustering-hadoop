package ru.hse.tochilkin.multiclustering.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdReducer extends Reducer<Text, Text, Text, Text>{
	
	
	@Override
	public void reduce(Text cluster, Iterable<Text> generatingRelations, 
			Context context) throws IOException, InterruptedException {
		context.write(cluster, new Text());
	}

}
