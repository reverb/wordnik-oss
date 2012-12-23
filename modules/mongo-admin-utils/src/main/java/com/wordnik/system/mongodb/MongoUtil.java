// Copyright (C) 2012  Wordnik, Inc.
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

import java.io.File;
import java.util.List;

public abstract class MongoUtil {
	protected void selectCollections(String collectionString, List<String> collectionsToAdd, List<String> collectionsToSkip){
		if(collectionString != null){
			boolean hasIncludes = false;
			String[] collectionNames = collectionString.split(",");
			for(String collectionName : collectionNames){
				if(collectionName.startsWith("!")){
					//	skip it
					collectionsToSkip.add(collectionName.substring(1).trim());
				}
				else{
					collectionsToAdd.add(collectionName.trim());
					hasIncludes = true;
				}
			}
			if(!hasIncludes){
				collectionsToAdd.add("*");
			}
		}
		else{
			collectionsToAdd.add("*");
		}
	}

	protected static void validateDirectory(String directory){
		File dir = new File(directory);
		if(!dir.exists()){
			if(!dir.mkdir()){
				throw new RuntimeException("can't create output dir");
			}
		}
	}
}