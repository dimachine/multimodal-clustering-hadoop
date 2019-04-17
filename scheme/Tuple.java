package ru.hse.tochilkin.multiclustering.scheme;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Tuple implements WritableComparable<Tuple>{
	private Entity[] entities;
	private int dim;
	
	public Tuple(Entity[] ents) {
		entities = ents;
		dim = entities.length;
	}
	
	public Tuple(String[] vals) {
		dim = vals.length;
		entities = new Entity[dim];
		for (int i = 0; i < dim; ++i) {
			Entity entity = new Entity(i, vals[i]);
			entities[i] = entity;
		}
	}
	
	public Tuple(Text text) {
		String[] vals = text.toString().split(",");
		dim = vals.length;
		entities = new Entity[dim];
		for (int i = 0; i < dim; ++i) {
			Entity entity = new Entity(i, vals[i]);
			entities[i] = entity;
		}
	}
	
	@Override
    public void write(DataOutput output) throws IOException {
		output.writeInt(dim);
        for (Entity entity : entities) {
            entity.write(output);
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
    	dim = input.readInt();
    	entities = new Entity[dim];
        for (int i = 0; i < dim; i++) {
        	Entity entity = new Entity();
            entity.readFields(input);
            entities[i] = entity;
        }
    }
    
    @Override
    public String toString() {
    	return "{" + getRecord() + "}";
    }
    
    public String getRecord() {
    	StringBuilder stringBuilder = new StringBuilder();
    	for (Entity entity: entities) {
			stringBuilder.append(entity.getValue()).append(',');
		}
		stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		return stringBuilder.toString();
    }
    
    public Text toText() {
    	return new Text(getRecord());
    }

	@Override
	public int compareTo(Tuple o) {
		return toString().compareTo(o.toString());
	}

	public void setElement(int index, String value) {
		entities[index].set(index, value);
	}

	public void setElement(Entity entity) {
		entities[entity.getIndex()] = entity;
	}
}
