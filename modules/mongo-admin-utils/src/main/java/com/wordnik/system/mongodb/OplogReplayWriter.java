package com.wordnik.system.mongodb;

import com.wordnik.mongo.connection.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.bson.BasicBSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class OplogReplayWriter implements OplogRecordProcessor {
	protected static Map<String, String> COLLECTION_MAPPING = new HashMap<String, String>();
	protected static Map<String, String> DATABASE_MAPPING = new HashMap<String, String>();
	protected static Map<String, String> NAMESPACE_COLLECTION_MAP = new HashMap<String, String>();
	protected static Map<String, String> UNMAPPED_NAMESPACE_COLLECTION_MAP = new HashMap<String, String>();

	protected String destinationDatabaseUsername;
	protected String destinationDatabasePassword;
	protected String destinationDatabaseHost;

	protected long insertCount;
	protected long updateCount;
	protected long deleteCount;
	protected long commandCount;

	protected Boolean accumulationMode = false;

	public void addDatabaseMapping(String src, String dst){
		DATABASE_MAPPING.put(src, dst);
	}
	
	public void setDatabaseMappings(Map<String, String> mappings){
		DATABASE_MAPPING = mappings;
	}
	
	public void addCollectionMapping(String src, String dst){
		COLLECTION_MAPPING.put(src, dst);
	}
	
	public void setCollectionMappings(Map<String, String> mappings){
		COLLECTION_MAPPING = mappings;
	}

	public long getInsertCount() {
		return insertCount;
	}

	public long getUpdateCount() {
		return updateCount;
	}

	public long getDeleteCount() {
		return deleteCount;
	}

	public long getCommandCount() {
		return commandCount;
	}

	public String getDestinationDatabaseUsername() {
		return destinationDatabaseUsername;
	}

	public void setDestinationDatabaseUsername(String destinationDatabaseUsername) {
		this.destinationDatabaseUsername = destinationDatabaseUsername;
	}

	public String getDestinationDatabasePassword() {
		return destinationDatabasePassword;
	}

	public void setDestinationDatabasePassword(String destinationDatabasePassword) {
		this.destinationDatabasePassword = destinationDatabasePassword;
	}

	public String getDestinationDatabaseHost() {
		return destinationDatabaseHost;
	}

	public void setDestinationDatabaseHost(String destinationDatabaseHost) {
		this.destinationDatabaseHost = destinationDatabaseHost;
	}

	public void setAccumulationMode() {
		this.accumulationMode = true;
	}

	@Override
	public void processRecord(BasicDBObject dbo) throws Exception {
		String operationType = dbo.getString("op");
		String namespace = dbo.getString("ns");
		String targetCollection = getMappedCollectionFromNamespace(namespace);
		BasicDBObject operation = new BasicDBObject((BasicBSONObject)dbo.get("o"));
		String targetDatabase = getDatabaseMapping(namespace);

		if(shouldProcessRecord(targetDatabase, targetCollection)){
			DB db = MongoDBConnectionManager.getConnection("REPLAY", destinationDatabaseHost, targetDatabase, destinationDatabaseUsername, destinationDatabasePassword, SchemaType.READ_WRITE());
			DBCollection coll = db.getCollection(targetCollection);
	
			try{
				if("i".equals(operationType)){
					insertCount++;
					coll.insert(operation);
				}
				else if("d".equals(operationType)){
					if(!accumulationMode)
					{
						deleteCount++;
						coll.remove(operation);
					}
				}
				else if("u".equals(operationType)){
					updateCount++;
					BasicDBObject o2 = new BasicDBObject((BasicBSONObject)dbo.get("o2"));
					coll.update(o2, operation);
				}
				else if("c".equals(operationType)){
					commandCount++;
					db.command(operation);
				}
			}
			catch (Exception e) {
				System.out.println("failed to process record " + operation.toString());
			}
		}
	}

	protected boolean shouldProcessRecord(String database, String collection){
		if(database != null && collection != null){
			return true;
		}
		return false;
	}
	
	public String getUnmappedCollectionFromNamespace(String namespace) {
		if(UNMAPPED_NAMESPACE_COLLECTION_MAP.containsKey(namespace)){
			return UNMAPPED_NAMESPACE_COLLECTION_MAP.get(namespace);
		}
		String[] parts = namespace.split("\\.");
		if(parts == null || parts.length == 1){
			return null;
		}
		String collection = null;
		if(parts.length == 2){
			collection = parts[1];
		}
		else{
			collection = namespace.substring(parts[0].length()+1);
		}
		
		UNMAPPED_NAMESPACE_COLLECTION_MAP.put(namespace, collection);

		return collection;
	}

	/**
	 * returns a collection name from FQ namespace.  Assumes database name never has "." in it.
	 * 
	 * @param namespace
	 * @return
	 */
	public String getMappedCollectionFromNamespace(String namespace) {
		if(NAMESPACE_COLLECTION_MAP.containsKey(namespace)){
			return NAMESPACE_COLLECTION_MAP.get(namespace);
		}
		String[] parts = namespace.split("\\.");
		if(parts == null || parts.length == 1){
			return null;
		}
		String collection = null;
		if(parts.length == 2){
			collection = parts[1];
		}
		else{
			collection = namespace.substring(parts[0].length()+1);
		}
		
		collection = remapCollection(collection);
		NAMESPACE_COLLECTION_MAP.put(namespace, collection);

		return collection;
	}

	/**
	 * remaps a collection if mapping exists, returns original if not
	 * 
	 * @param collection
	 * @return
	 */
	public String remapCollection(String collection){
		String o = COLLECTION_MAPPING.get(collection);
		return o == null ? collection:o;
	}

	/**
	 * returns a database name from FQ namespace.  Assumes database name never has "." in it.
	 * 
	 * @param namespace
	 * @return
	 */
	public String getDatabaseMapping (String namespace) {
		String[] parts = namespace.split("\\.");
		if(parts == null || parts.length == 1){
			return null;
		}
		String databaseName = parts[0];
		databaseName = remapDatabase(databaseName);
		return databaseName;
	}

	/**
	 * remaps a database name if mapping exists, returns original if not
	 * 
	 * @param databaseName
	 * @return
	 */
	public String remapDatabase(String databaseName){
		String o = DATABASE_MAPPING.get(databaseName);
		return o == null ? databaseName:o;
	}

	@Override
	public void close(String string) throws IOException {}
}
