package create;

import java.io.IOException;
import java.util.Properties;

import com.mongodb.DBCollection;
import com.mongodb.MongoException;

import config.ConnectMongoDB;
import config.ReadProperties;

public class MongoDBCreate {
	
	private ConnectMongoDB conMongoDB;
	
	public MongoDBCreate() throws IOException {
		this.conMongoDB = new ConnectMongoDB();
	}

	/*
	 * Metodo pega  a collection desejada
	 * caso nao exista, ela cria uma nova
	 */
	public DBCollection getCollection() throws IOException{
		DBCollection coll = this.conMongoDB.connectToMongoDB();
		return coll;
	}
	
	/*
	 * Metodo deleta a collection
	 */
	public void dropCollection() throws IOException{
		
		DBCollection coll = this.conMongoDB.connectToMongoDB();
		coll.drop();
	}
	
	public static void main(String[] args) throws IOException {
		try {
			System.out.println("*** Creating MongoDB Test");
			Properties prop = ReadProperties.getProp();
			String collection = prop.getProperty("mongodb.collection");
			
			MongoDBCreate mongodbCreate = new MongoDBCreate();
			
			System.out.println("Dropping collection "+collection);
			mongodbCreate.dropCollection();
			System.out.println("OK");
			
			System.out.println("Creating collection "+collection);
			mongodbCreate.getCollection();
			System.out.println("OK");

		}  catch (MongoException e) {
			e.printStackTrace();
		}

	}
}
