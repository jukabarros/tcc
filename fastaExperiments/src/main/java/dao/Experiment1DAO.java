package dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import config.ConnectCassandra;

public class Experiment1DAO {
	
	private ConnectCassandra connCassandra;
	private Session session;
	private String query;
	private final String KEYSPACE = "fastaExperiment1";
	
	public Experiment1DAO() {
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
		System.out.println(String.format("%-100s", "all_data",
				"--------------------------------------------------------"));
		for (Row row : results) {
			line++;
			System.out.println(String.format("%-100s", row.getString("all_data")));
		}
		System.out.println();
		System.out.println("******* NUM OF LINES: "+line);
		this.connCassandra.close();
	}
	
	public void insert(String data){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect(KEYSPACE);
		try{
			
			this.query = "INSERT INTO fastaCollect (all_data) VALUES (?);";
			PreparedStatement statement = this.session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			this.session.execute(boundStatement.bind(data));
			
		}catch (Exception e){
			System.out.println("Erro ao executar a query: :("+e.getMessage());
		}
		this.connCassandra.close();
		
	}
	
	

}
