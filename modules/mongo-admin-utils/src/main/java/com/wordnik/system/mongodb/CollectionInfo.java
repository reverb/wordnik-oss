// Copyright (C) 2010  Wordnik, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your 
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty 
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser 
// General Public License for more details.  You should have received a copy 
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.

package com.wordnik.system.mongodb;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;

/**
 * Stores info about a collection
 * 
 * @author tony
 *
 */
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