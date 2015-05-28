package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import config.ConnectMySQL;

public class MySQLDAO {
	
	private String query;
	
	private Connection conn;
	
	public MySQLDAO() {
		this.query = null;
		this.conn = null;
	}

	public void beforeExecuteQuery(){
		this.conn = new ConnectMySQL().connectMysql();
	}
	
	public void afterExecuteQuery() throws SQLException{
		this.conn.close();
	}
	
	public void executeQuery(String query) throws SQLException{
		PreparedStatement queryExec = this.conn.prepareStatement(query);
		queryExec.execute();
		queryExec.close();
	}

	public void insertData(String id, String seqDna) throws SQLException{
		try{
			query = "INSERT INTO fasta_collect (id, seq_dna) VALUES (?,?);";
			PreparedStatement queryExec = this.conn.prepareStatement(query);
			queryExec.setString(1, id);
			queryExec.setString(2, seqDna);
			queryExec.execute();
			queryExec.close();
		}catch (Exception e){
			System.out.println("Erro ao inserir o registro: :( \n"+e.getMessage());
		}
	}
	
	public void findAll() throws SQLException{
		query = "SELECT * FROM fasta_collect;";
		PreparedStatement queryExec = this.conn.prepareStatement(query);
		ResultSet results = queryExec.executeQuery();
		int line = 0;
		System.out.println(String.format("%-30s\t%-70s", "id", "seqDNA",
				"----------------+------------------------------------"));
		while (results.next()){
			System.out.println(String.format("%-30s\t%-70s", results.getString(1), results.getString(2)));
			line++;
		}
		System.out.println();
		System.out.println("******* NUM OF LINES: "+line);
		
	}
	
	public void findByID(String id) throws SQLException{
		query = "SELECT * FROM fasta_collect WHERE id = ?";
		PreparedStatement queryExec = this.conn.prepareStatement(query);
		queryExec.setString(1, id);
		ResultSet results = queryExec.executeQuery();
		int line = 0;
		System.out.println(String.format("%-30s\t%-70s", "id", "seqDNA",
				"----------------+------------------------------------"));
		while (results.next()){
			System.out.println(String.format("%-30s\t%-70s", results.getString(1), results.getString(2)));
			line++;
		}
		System.out.println();
		System.out.println("******* NUM OF LINES: "+line);
		
	}

}
