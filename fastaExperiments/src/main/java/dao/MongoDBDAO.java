package dao;

import java.io.IOException;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import create.MongoDBCreate;

public class MongoDBDAO {
	
	private MongoDBCreate mongoDBCreate;
	private DBCollection dbCollection;
	
	public MongoDBDAO() throws IOException {
		super();
		this.mongoDBCreate = new MongoDBCreate();
		this.dbCollection =  this.mongoDBCreate.getCollection();
	}
	
	public void insertData(String id, String seqDna) throws IOException{

		/**** Insert ****/
		// create a document to store key and value
		BasicDBObject document = new BasicDBObject();
		document.put("id", id);
		document.put("seqDna", seqDna);
		document.put("createdDate", new Date());
		this.dbCollection.insert(document);
	}
	
	public void findByID(String id) throws IOException{
		/**** Find and display ****/
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("id", id);

		DBCursor cursor = this.dbCollection.find(searchQuery);

		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	
	
	public void findAll(String outputfile) throws IOException{

		DBCursor cursor = this.dbCollection.find();

		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}

}
