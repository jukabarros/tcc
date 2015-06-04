package dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import config.ConnectMySQL;
import config.ReadProperties;
import dna.FastaContent;

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
		queryExec = null;
	}

	public void insertFastaInfo(String fileName, long size, String comment) throws SQLException{
		try{
			beforeExecuteQuery();
			
			query = "INSERT INTO fasta_info (file_name, size, comment) VALUES (?,?,?);";
			PreparedStatement queryExec = this.conn.prepareStatement(query);
			queryExec.setString(1, fileName);
			queryExec.setLong(2, size);
			queryExec.setString(3, comment);
			queryExec.execute();
			queryExec.close();
			
			afterExecuteQuery();
		}catch (Exception e){
			System.out.println("Erro ao inserir o registro: :( \n"+e.getMessage());
		}
	}
	public int getIDFastaInfo(String fileName) throws SQLException{
		beforeExecuteQuery();
		
		query = "SELECT * FROM fasta_info WHERE file_name = ?";
		PreparedStatement queryExec = this.conn.prepareStatement(query);
		queryExec.setString(1, fileName);
		ResultSet results = queryExec.executeQuery();
		int id = 0;
		while (results.next()){
			id = results.getInt(1);
		}
		
		afterExecuteQuery();
		
		return id;
		
	}
	
	/*
	 * Metodos de consulta ao banco de dados retornam uma lista de FastaInfo
	 * o qual é usado para gerar o arquivo de saida
	 */
	
	public List<FastaContent> findAll() throws SQLException{
		beforeExecuteQuery();
		
		query = "SELECT * FROM fasta_collect;";
		PreparedStatement queryExec = this.conn.prepareStatement(query);
		ResultSet results = queryExec.executeQuery();
		int line = 0;
		List<FastaContent> listFastaInfo = new ArrayList<FastaContent>();
		while (results.next()){
			FastaContent fastaInfo = new FastaContent(results.getString(1), results.getString(2), results.getInt(3));
			listFastaInfo.add(fastaInfo);
			fastaInfo = null;
			line++;
		}
		
		afterExecuteQuery();
		
		System.out.println();
		System.out.println("***** Quantidade de registros: "+line);
		return listFastaInfo;
		
	}
	
	/**
	 * Retorna o conteudo de um arquivo especifico
	 * @param fileName
	 * @return
	 * @throws SQLException
	 */
	public List<FastaContent> findByFilename(String fileName) throws SQLException{
		// Recuperando o id do arquivo
		int fileID = this.getIDFastaInfo(fileName);
		
		beforeExecuteQuery();		
		query = "SELECT id_seq, seq_dna, line FROM fasta_collect WHERE fasta_info = ?;";
		PreparedStatement queryExec = this.conn.prepareStatement(query);
		queryExec.setInt(1, fileID);
		ResultSet results = queryExec.executeQuery();
		int line = 0;
		List<FastaContent> listFastaInfo = new ArrayList<FastaContent>();
		while (results.next()){
			FastaContent fastaInfo = new FastaContent(results.getString(1), results.getString(2), results.getInt(3));
			listFastaInfo.add(fastaInfo);
			fastaInfo = null;
			line++;
		}
		if (listFastaInfo.isEmpty()){
			System.out.println("*** Conteúdo do arquivo não encontrado no Banco de dados :(");
		}
		afterExecuteQuery();
		
		System.out.println();
		System.out.println("**** Quantidade de registros: "+line);
		return listFastaInfo;
		
	}
	
	public List<FastaContent> findByID(String idSeqDNA) throws SQLException{
		
		beforeExecuteQuery();	
		query = "SELECT id_seq, seq_dna, line FROM fasta_collect WHERE id_seq = ?";
		PreparedStatement queryExec = this.conn.prepareStatement(query);
		queryExec.setString(1, idSeqDNA);
		ResultSet results = queryExec.executeQuery();
		int line = 0;
		List<FastaContent> listFastaInfo = new ArrayList<FastaContent>();
		while (results.next()){
			FastaContent fastaInfo = new FastaContent(results.getString(1), results.getString(2), results.getInt(3));
			listFastaInfo.add(fastaInfo);
			fastaInfo = null;
			line++;
		}
		if (listFastaInfo.isEmpty()){
			System.out.println("*** ID não encontrado no Banco de dados :(");
		}
		afterExecuteQuery();
		System.out.println();
		System.out.println("**** Quantidade de linhas: "+line);
		
		return listFastaInfo;
		
	}

}
