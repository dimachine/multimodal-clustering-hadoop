package ru.hse.tochilkin.multiclustering.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ru.hse.tochilkin.multiclustering.params.Constants;

public class ThirdMapper extends Mapper<Text, Text, Text, Text>{
	private int numKeys;
	private int modality;
	
	@Override
    protected void setup(Context context) 
    		throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		numKeys = configuration.getInt(Constants.entityDelimeterParameter, Constants.defaultNumKeys);
		modality = configuration.getInt(Constants.modalityParameter, Constants.defaultModality);
	}
	
	
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(value, key);
	}

}
