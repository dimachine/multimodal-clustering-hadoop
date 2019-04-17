package ru.hse.tochilkin.multiclustering.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import ru.hse.tochilkin.multiclustering.params.Constants;
import ru.hse.tochilkin.multiclustering.scheme.Cumulus;
import ru.hse.tochilkin.multiclustering.scheme.FormalConcept;

public class SecondReducer 
		extends Reducer<Text, Cumulus, Text, Text>{
	
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
	public void reduce(Text relation, Iterable<Cumulus> concept, 
			Context context) throws IOException, InterruptedException {
		//System.out.println("concept:");
		//for (Delate d: concept) {
			//System.out.println(d + "   index: " + d.getIndex());
		//}
		ArrayList<Cumulus> cArrayList = new ArrayList<>();
		for (Cumulus d: concept) {
			cArrayList.add(new Cumulus(d.getIndex(), d.getValues()));
		}
		FormalConcept formalConcept = new FormalConcept(cArrayList); //,modality);
		
		context.write(relation, formalConcept.toText());
	}
}

