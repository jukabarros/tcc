package dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import config.ConnectCassandra;
import config.ReadProperties;
import dna.FastaInfo;

public class CassandraDAO {
	
	private ConnectCassandra connCassandra;
	private Session session;
	private String query;
	private String keyspace;
	
	public CassandraDAO() throws IOException {
		super();
		this.connCassandra = new ConnectCassandra();
		this.session = null;
		this.query = null;
		Properties prop = ReadProperties.getProp();
		this.keyspace =  prop.getProperty("cassandra.keyspace");
	}
	
	public void beforeExecuteQuery(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect(this.keyspace);
		
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
	
	public void findAll(){
		this.beforeExecuteQuery();
		this.query = "SELECT * FROM fastaCollect;";
		ResultSet results = session.execute(this.query);
		int line = 0;
//		System.out.println(String.format("%-30s\t%-70s", "id", "seqDNA",
//				"----------------+------------------------------------"));
		List<FastaInfo> listFastaInfo = new ArrayList<FastaInfo>();
		for (Row row : results) {
//			System.out.println(String.format("%-30s\t%-70s", row.getString("id"), row.getString("seq_dna")));
			FastaInfo fastaInfo = new FastaInfo(row.getString("id"), row.getString("seq_dna"), row.getInt(3));
			listFastaInfo.add(fastaInfo);
			fastaInfo = null;
			line++;
		}
		this.afterExecuteQuery();
		System.out.println();
		System.out.println("******* Quantidade de linhas: "+line);
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
//		System.out.println(String.format("%-30s\t%-70s", "id", "seqDNA",
//				"----------------+------------------------------------"));
		List<FastaInfo> listFastaInfo = new ArrayList<FastaInfo>();
		for (Row row : results) {
//			System.out.println(String.format("%-30s\t%-70s", row.getString("id"), row.getString("seq_dna")));
			FastaInfo fastaInfo = new FastaInfo(row.getString("id"), row.getString("seq_dna"), row.getInt("num_line"));
			listFastaInfo.add(fastaInfo);
			fastaInfo = null;
			line++;
		}
		this.afterExecuteQuery();
		System.out.println();
		System.out.println("******* Quantidade de linhas: "+line);
	}

}
