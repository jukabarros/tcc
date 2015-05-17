package dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import config.ConnectCassandra;

public class Experiment2DAO {
	
	private ConnectCassandra connCassandra;
	private Session session;
	private String query;
	private final String KEYSPACE = "fastaExperiment2";
	
	public Experiment2DAO() {
		super();
		// TODO Auto-generated constructor stub
		this.connCassandra = new ConnectCassandra();
		this.session = null;
		this.query = null;
	}
	
	public void selectAll(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect(KEYSPACE);
		ResultSet results = session.execute("SELECT * FROM fastaCollect;");
		int line = 0;
		System.out.println(String.format("%-30s\t%-70s", "id", "seqDNA",
				"----------------+------------------------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-30s\t%-70s", row.getString("id"), row.getString("seq_dna")));
		}
		System.out.println();
		System.out.println("******* NUM OF LINES: "+line);
		this.connCassandra.close();
	}
	
	public void insert(String id, String seqDna){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect(KEYSPACE);
		try{
			
			this.query = "INSERT INTO fastaCollect (id, seq_dna) VALUES (?, ?);";
			PreparedStatement statement = this.session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			this.session.execute(boundStatement.bind(id, seqDna));
			
		}catch (Exception e){
			System.out.println("Erro ao executar a query: :("+e.getMessage());
		}
		this.connCassandra.close();
		
	}
	
	public void findByID(String id){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect(KEYSPACE);
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
		System.out.println();
		System.out.println("******* NUM OF LINES: "+line);
		this.connCassandra.close();
	}
	
	

}
