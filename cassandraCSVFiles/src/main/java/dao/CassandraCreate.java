package dao;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.datastax.driver.core.Session;

import config.ConnectCassandra;

public class CassandraCreate {
	
	private ConnectCassandra connCassandra;
	private Session session;
	private String query;
	
	public CassandraCreate() {
		super();
		this.connCassandra = new ConnectCassandra();
		this.session = null;
		this.query = null;
	}

	public void createKeyspace(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		System.out.println("Creating keyspace specdata");
		try{
			this.query = "CREATE KEYSPACE specdata WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao criar o keyspace: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public void createTables(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		try{
			this.query = "CREATE TABLE specdata.collect (" +
            "dateOfColect timestamp PRIMARY KEY," + 
            "sulfate text, "
            + "nitrate text, "
            + "id int)";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao criar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		CassandraCreate create = new CassandraCreate();
		System.out.println("Creating tables...");
		create.createTables();
		System.out.println("OK");
		long endTime = System.currentTimeMillis();
		
		long totalTime = endTime - startTime;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("******** EXECUTION TIME: " + formatter.format(totalTime / 1000d) + " seconds");
	}
}
