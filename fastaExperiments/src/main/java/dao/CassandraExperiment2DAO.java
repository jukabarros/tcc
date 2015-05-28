package dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import config.ConnectCassandra;

public class CassandraExperiment2DAO {
	
	private ConnectCassandra connCassandra;
	private Session session;
	private String query;
	private final String KEYSPACE = "experiment2";
	
	public CassandraExperiment2DAO() {
		super();
		// TODO Auto-generated constructor stub
		this.connCassandra = new ConnectCassandra();
		this.session = null;
		this.query = null;
	}
	
	public void beforeExecuteQuery(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect(KEYSPACE);
		
	}
	
	public void afterExecuteQuery(){
		this.connCassandra.close();
	}
	
	public void executeQuery(String query){
		try{
			this.session.execute(query);
		}catch (Exception e){
			System.out.println("Erro ao executar a query: :("+e.getMessage());
		}
	}
	
	public void selectAll(){
		this.beforeExecuteQuery();
		this.query = "SELECT * FROM fastaCollect;";
		ResultSet results = session.execute(this.query);
		int line = 0;
		System.out.println(String.format("%-30s\t%-70s", "id", "seqDNA",
				"----------------+------------------------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-30s\t%-70s", row.getString("id"), row.getString("seq_dna")));
			line++;
		}
		this.afterExecuteQuery();
		System.out.println();
		System.out.println("******* NUM OF LINES: "+line);
	}
	
	public void insert(String id, String seqDna){
		try{
			
			this.query = "INSERT INTO fastaCollect (id, seq_dna) VALUES (?, ?);";
			PreparedStatement statement = this.session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			this.session.execute(boundStatement.bind(id, seqDna));
			
		}catch (Exception e){
			System.out.println("Erro ao executar a query: :("+e.getMessage());
		}
	}
	
	public void findByID(String id){
		this.beforeExecuteQuery();
		this.query = "SELECT * FROM fastaCollect WHERE id = ?;";
		PreparedStatement statement = this.session.prepare(query);
		BoundStatement boundStatement = new BoundStatement(statement);
		ResultSet results = this.session.execute(boundStatement.bind(id));
		int line = 0;
		System.out.println(String.format("%-30s\t%-70s", "id", "seqDNA",
				"----------------+------------------------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-30s\t%-70s", row.getString("id"), row.getString("seq_dna")));
		}
		this.afterExecuteQuery();
		System.out.println();
		System.out.println("******* NUM OF LINES: "+line);
	}

}
