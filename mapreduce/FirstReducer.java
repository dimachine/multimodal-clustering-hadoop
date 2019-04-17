package ru.hse.tochilkin.multiclustering.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import ru.hse.tochilkin.multiclustering.scheme.Entity;
import ru.hse.tochilkin.multiclustering.scheme.Cumulus;
import ru.hse.tochilkin.multiclustering.scheme.Tuple;

public class FirstReducer extends 
		Reducer<Text, Entity, Text, Text>{
	
	@Override
	public void reduce(Text subTuple, Iterable<Entity> entities,
			Context context) throws IOException, InterruptedException {
		int index = 0;
		int entitiesNumber = 0;
		Iterator<Entity> iterator = entities.iterator();
		ArrayList<Entity> entityList = new ArrayList<Entity>();
		while (iterator.hasNext()) {
			/*if (e.getIndex() != index) {
				//log error
				//return;
				;
			}*/
			
			Entity e = iterator.next();
			entityList.add(new Entity(e.getIndex(), e.getValue()));
			index = e.getIndex();
			++entitiesNumber;
		}
		
		String[] entityArray = new String[entitiesNumber];//delate for a given subrelation
		for (int i = 0; i < entitiesNumber; ++i) {
			entityArray[i] = entityList.get(i).getValue();
		}
		
		Cumulus conceptEntities = new Cumulus(index, entityArray);
		context.write(subTuple, conceptEntities.toText());
		
		/*for (int j = 0; j < entitiesNumber; ++j) {
			
			tuple.setElement(entityArray[j]);
			context.write(tuple, conceptEntities);
		}*/
	}
}
