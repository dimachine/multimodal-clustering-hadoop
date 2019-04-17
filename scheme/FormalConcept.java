package ru.hse.tochilkin.multiclustering.scheme;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.Text;

public class FormalConcept {
	ArrayList<Cumulus> concept;	
	
	public FormalConcept(ArrayList<Cumulus> delates) {
		concept = delates;
		Collections.sort(concept);
	}
	
	public Text toText() {
		return new Text(toString());
	}
	
	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("{" + concept.get(0));
		for (int i = 1; i < concept.size(); ++i) {
			stringBuilder.append(", " + concept.get(i).toString());
		}
		
		return stringBuilder.append('}').toString();
	}
}
