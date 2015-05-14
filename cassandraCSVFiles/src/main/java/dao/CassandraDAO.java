package dao;

import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import config.ConnectCassandra;

public class CassandraDAO {
	
	private ConnectCassandra connCassandra;
	private Session session;
	private String query;
	private final String KEYSPACE = "specdata";
	
	public CassandraDAO() {
		super();
		this.connCassandra = new ConnectCassandra();
		this.session = null;
		this.query = null;
	}

	public void selectAll(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect(KEYSPACE);
		ResultSet results = session.execute("SELECT * FROM collect;");
		System.out.println(String.format("%-20s\t%-30s\t%-30s\t%-20s", "dateOfColect", "sulfate", "nitrate", "ID",
				"-------------------------------+-----------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-20s\t%-30s\t%-30s\t%-20s", row.getDate("dateOfColect")
					, row.getString("sulfate"), row.getString("nitrate"), row.getInt("ID")));
		}
		System.out.println();
		
		this.connCassandra.close();
	}
	
	public void insert(Date dateOfColect, String sulfate, String nitrate, int id){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect(KEYSPACE);
		try{
			
			this.query = "INSERT INTO collect (dateOfColect,sulfate,nitrate,id) VALUES (?,?,?,?);";
			PreparedStatement statement = this.session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			this.session.execute(boundStatement.bind(dateOfColect, sulfate, nitrate, id));
			
		}catch (Exception e){
			System.out.println("Erro ao executar a query: :("+e.getMessage());
		}
		this.connCassandra.close();
		
	}
	
	public void delete(Date date){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect(KEYSPACE);
		try{
			this.query = "DELETE FROM collect WHERE dateOfColect = ?";
			PreparedStatement statement = this.session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			this.session.execute(boundStatement.bind(date));
		}catch (Exception e){
			System.out.println("Erro ao executar a query: "+e.getMessage());
		}
		this.connCassandra.close();
		
	}
	
	public void truncate(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect(KEYSPACE);
		try{
			this.query = "TRUNCATE collect";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao executar a query: "+e.getMessage());
		}
		this.connCassandra.close();
		
	}
	
	/*
	 * Nao sera usado
	 */
	public void update(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect(KEYSPACE);
		try{
			this.query = "UPDATE table1 SET title = ? WHERE id = ?";
			PreparedStatement statement = this.session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			this.session.execute(boundStatement.bind("tabela numero 1", UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50")));
		}catch (Exception e){
			System.out.println("Erro ao executar a query: "+e.getMessage());
		}
		this.connCassandra.close();
		
	}
	
}
