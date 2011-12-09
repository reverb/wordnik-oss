package com.wordnik.system.mongodb;

import java.io.IOException;

import com.mongodb.BasicDBObject;

public interface OplogRecordProcessor {
	void processRecord(BasicDBObject x) throws Exception;
	void close(String string) throws IOException;
}
