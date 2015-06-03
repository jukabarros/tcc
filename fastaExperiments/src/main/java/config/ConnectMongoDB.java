package config;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class ConnectMongoDB {

	private MongoClient mongo;
	private String database;
	private Properties prop;
	
	public ConnectMongoDB() throws IOException {
		super();
		this.mongo = null;
		this.database = null;
		this.prop = ReadProperties.getProp();
	}
	
	/*
	 * Conecta com o Mongo e aponta para a colecao,
	 * caso ela nao exista, o mongo por padrao cria uma nova.
	 */
	public DBCollection connectToMongoDB() throws IOException{
		String host = this.prop.getProperty("mongodb.host");
		int port = Integer.parseInt(this.prop.getProperty("mongodb.port"));
		this.database = prop.getProperty("mongodb.db");
		this.mongo = new MongoClient(host, port);
		String collection = this.prop.getProperty("mongodb.collection");
		
		DB db = this.mongo.getDB(this.database);
		DBCollection dbCollection = db.getCollection(collection);
		
		return dbCollection; 
	}

	public static void main(String[] args) throws IOException {
		try {

			ConnectMongoDB mongoCon = new ConnectMongoDB();
			DBCollection table = mongoCon.connectToMongoDB();

			/**** Insert ****/
			// create a document to store key and value
			BasicDBObject document = new BasicDBObject();
			document.put("name", "juka");
			document.put("age", 24);
			document.put("createdDate", new Date());
			table.insert(document);

			/**** Find and display ****/
			BasicDBObject searchQuery = new BasicDBObject();
			searchQuery.put("name", "juka");

			DBCursor cursor = table.find(searchQuery);

			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}

			/**** Update ****/
			// search document where name="juka" and update it with new values
			BasicDBObject query = new BasicDBObject();
			query.put("name", "juka");

			BasicDBObject newDocument = new BasicDBObject();
			newDocument.put("name", "juka-updated");

			BasicDBObject updateObj = new BasicDBObject();
			updateObj.put("$set", newDocument);

			table.update(query, updateObj);

			/**** Find and display ****/
			BasicDBObject searchQuery2 = new BasicDBObject().append("name", "juka-updated");

			DBCursor cursor2 = table.find(searchQuery2);

			while (cursor2.hasNext()) {
				System.out.println(cursor2.next());
			}

			/**** Done ****/
			System.out.println("Done");

		}  catch (MongoException e) {
			e.printStackTrace();
		}

	}


}

