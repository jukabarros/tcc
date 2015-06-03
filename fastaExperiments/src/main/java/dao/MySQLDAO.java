package dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import config.ConnectMySQL;
import config.ReadProperties;

public class MySQLDAO {
	
	private String query;
	
	private Connection conn;
	
	public MySQLDAO() {
		this.query = null;
		this.conn = null;
	}
	
	/*
	 * Metodo before e after sao usados para
	 * abrir e fechar conexao
	 */
	public void beforeExecuteQuery(){
		Properties prop;
		try {
			prop = ReadProperties.getProp();
			String database = prop.getProperty("mysql.db"); 
			this.conn = new ConnectMySQL().connectMysql(database);
		} catch (IOException e) {
			System.out.println("Erro na execução da query: "+e.getMessage());
		}
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
			beforeExecuteQuery();
			
			query = "INSERT INTO fasta_collect (id, seq_dna) VALUES (?,?);";
			PreparedStatement queryExec = this.conn.prepareStatement(query);
			queryExec.setString(1, id);
			queryExec.setString(2, seqDna);
			queryExec.execute();
			queryExec.close();
			
			afterExecuteQuery();
		}catch (Exception e){
			System.out.println("Erro ao inserir o registro: :( \n"+e.getMessage());
		}
	}
	
	/*
	 * outputfile eh o arquivo fasta que vai ser gerado apos a consulta (TO DO)
	 */
	public void findAll(String outputfile) throws SQLException{
		beforeExecuteQuery();
		
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
		
		afterExecuteQuery();
		System.out.println();
		System.out.println("***** Quantidade de registros: "+line);
		
	}
	
	public void findByID(String id) throws SQLException{
		beforeExecuteQuery();
		
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
		
		afterExecuteQuery();
		System.out.println();
		System.out.println("***** Quantidade de linhas: "+line);
		
	}

}
