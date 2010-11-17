package com.wordnik.system.mongodb;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;

public class CollectionInfo {
	private boolean collectionExists = true;
	private String name;
	private long count;
	private List<BasicDBObject> indexes = new ArrayList<BasicDBObject>();
	
	public CollectionInfo(String name, long count){
		this.name = name;
		this.count = count;
	}

	public void addIndex(BasicDBObject index) {
		indexes.add(index);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public List<BasicDBObject> getIndexes() {
		return indexes;
	}

	public void setIndexes(List<BasicDBObject> indexes) {
		this.indexes = indexes;
	}

	public boolean isCollectionExists() {
		return collectionExists;
	}

	public void setCollectionExists(boolean collectionExists) {
		this.collectionExists = collectionExists;
	}
}