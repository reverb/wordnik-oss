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

import java.util.HashMap;

import java.util.Map;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * maintains a cache of db connections
 * 
 * @author tony
 *
 */
public class DBConnector {
	private static Map<String, DB> DATABASE_CONNECTIONS = new HashMap<String, DB>();

	public static synchronized DB getDb(String host, String username, String password, String dbName) throws Exception {
		String dbIdentifier = host + "." + dbName;
		DB db = DATABASE_CONNECTIONS.get(dbIdentifier);
		if(db == null){
			Mongo m = new Mongo(host);
			if(username != null){
				db = m.getDB(dbName);
				db.authenticate(username, password.toCharArray());
			}
			else{
				db = m.getDB(dbName);
			}
			DATABASE_CONNECTIONS.put(dbIdentifier, db);
		}
		return db;
	}
}
