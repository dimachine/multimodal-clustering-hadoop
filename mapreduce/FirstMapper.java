package ru.hse.tochilkin.multiclustering.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ru.hse.tochilkin.multiclustering.exceptions.WrongModalityException;
import ru.hse.tochilkin.multiclustering.params.Constants;
import ru.hse.tochilkin.multiclustering.scheme.Entity;
import ru.hse.tochilkin.multiclustering.scheme.Tuple;

public class FirstMapper 
		extends Mapper<Object, Text, Text, Entity>{
	private String entityDelimeter;
	private int tupleSize;
	
	@Override
    protected void setup(Context context) 
    		throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		entityDelimeter = configuration.get(Constants.entityDelimeterParameter);
		tupleSize = configuration.getInt(Constants.modalityParameter, Constants.defaultModality);
	}
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tupleValues = value.toString().split(entityDelimeter);

		try {
			if (tupleValues.length != tupleSize) {
				throw new WrongModalityException(
						"Entered modality differs from modality of input data");
			}
		}
		catch (WrongModalityException e) {
			e.printStackTrace();
		}
		Tuple tuple = new Tuple(tupleValues);
		Entity entity = new Entity();
		for (int i = 0; i < tupleSize; ++i) {
			entity.set(i, tupleValues[i]);
			tuple.setElement(i, Constants.gap);
			context.write(tuple.toText(), entity);
			tuple.setElement(i, entity.getValue());
		}
	}
}
