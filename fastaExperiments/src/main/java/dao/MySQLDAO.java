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
import file.OutputFasta;

public class MySQLDAO{
	
	private String query;
	
	private Connection conn;
	
	public MySQLDAO() throws IOException {
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
	
	public void insertFastaCollect(String idSeq, String seqDNA, int line, int fastaInfo) throws SQLException{
		try{
			
			query = "INSERT INTO fasta_collect (id_seq, seq_dna, line, fasta_info) VALUES (?,?,?,?);";
			PreparedStatement queryExec = this.conn.prepareStatement(query);
			queryExec.setString(1, idSeq);
			queryExec.setString(2, seqDNA);
			queryExec.setInt(3, line);
			queryExec.setInt(4, fastaInfo);
			queryExec.execute();
			queryExec.close();
			
		}catch (Exception e){
			System.out.println("Erro ao inserir o registro: :( \n"+e.getMessage());
		}
	}
	
	/**
	 * Atualiza o numero de linhas existente em um arquivo
	 * apos a sua leitura.
	 * @param fileName
	 * @param numOfLines
	 * @throws SQLException
	 */
	public void updateNumOfLinesFastaInfo(String fileName, int numOfLines) throws SQLException{
		try{
			this.beforeExecuteQuery();
			this.query = "UPDATE fasta_info SET num_line = ? WHERE file_name = ?;";
			PreparedStatement queryExec = this.conn.prepareStatement(this.query);
			queryExec.setInt(1, numOfLines);
			queryExec.setString(2, fileName);
			queryExec.execute();
			queryExec.close();
			this.afterExecuteQuery();
		}catch (Exception e){
			System.out.println("Erro ao atualizar o numero de linhas :( \n"+e.getMessage());
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
	
	/**
	 * Metodo retorna o nome do arquivo que foi consultado por ID
	 * @param idFastaInfo
	 * @return
	 * @throws SQLException
	 */
	private String getFileNameFastaInfo(int idFastaInfo) throws SQLException{
		query = "SELECT * FROM fasta_info WHERE id = ?";
		PreparedStatement queryExec = this.conn.prepareStatement(query);
		queryExec.setInt(1, idFastaInfo);
		ResultSet results = queryExec.executeQuery();
		String fileName = null;
		while (results.next()){
			fileName = results.getString(2);
		}
		
		return fileName;
		
	}
	
	/**
	 * Metodo retorna o nome do arquivo que foi consultado por ID
	 * @param idFastaInfo
	 * @return
	 * @throws SQLException
	 */
	private Integer getNumOfLinesFastaInfo(int idFastaInfo) throws SQLException{
		this.query = "SELECT * FROM fasta_info WHERE id = ?";
		PreparedStatement queryExec = this.conn.prepareStatement(this.query);
		queryExec.setInt(1, idFastaInfo);
		ResultSet results = queryExec.executeQuery();
		int numOfLine = 0;
		while (results.next()){
			numOfLine = results.getInt(4);
		}
		
		return numOfLine;
		
	}
	
	/**
	 * Retorna o conteudo de um arquivo especifico
	 * @param fileName
	 * @return
	 * @throws SQLException
	 * @throws IOException 
	 */
	public void findByFilename(String fileName, int repeat) throws SQLException, IOException{
		// Recuperando o id do arquivo
		int fileID = this.getIDFastaInfo(fileName);
		OutputFasta outputFasta = new OutputFasta();
		beforeExecuteQuery();
		// recupera o numero total de linhas para ver se eh necessario fazer a extracao por partes
		Integer numOfLine = this.getNumOfLinesFastaInfo(fileID);
		System.out.println("*** Criando o arquivo: "+fileName);
		outputFasta.createFastaFile(repeat+fileName);
		if (numOfLine <= 500000){
			this.query = "SELECT TRIM(id_seq), TRIM(seq_dna) FROM fasta_collect WHERE fasta_info = ?;";
			PreparedStatement queryExec = this.conn.prepareStatement(this.query);
			queryExec.setInt(1, fileID);
			ResultSet results = queryExec.executeQuery();
			while (results.next()){
				outputFasta.writeFastaFile(results.getString(1), results.getString(2));
			}
		}else{
			int numOfRecords = 0;
			int numParts = numOfLine/500000;
			for (int i = 0; i < numParts; i++) {
				if (i == (numParts - 1)){
					this.query = "SELECT TRIM(id_seq), TRIM(seq_dna) FROM "
							+ "fasta_collect WHERE fasta_info = ? LIMIT "+numOfRecords+", "+numOfLine+";";
					
				}else{
					this.query = "SELECT TRIM(id_seq), TRIM(seq_dna) FROM "
							+ "fasta_collect WHERE fasta_info = ? LIMIT "+numOfRecords+", 500000;";
				}
				numOfRecords += 500000;
				PreparedStatement queryExec = this.conn.prepareStatement(this.query);
				queryExec.setInt(1, fileID);
				ResultSet results = queryExec.executeQuery();
				while (results.next()){
					outputFasta.writeFastaFile(results.getString(1), results.getString(2));
				}
				System.out.println("* Registros escritos: "+numOfRecords+"/"+numOfLine);

				queryExec = null;
				results = null;
				this.query = null;
			}
		}
		if (numOfLine.equals(0)){
			System.out.println("*** Conteúdo do arquivo não encontrado no Banco de dados :(");
		}
		
		outputFasta.closeFastaFile();
		afterExecuteQuery();
		
		System.out.println();
		System.out.println("**** Quantidade de registros: "+numOfLine);
		
	}
	
	public void findByID(String idSeqDNA) throws SQLException{
		
		beforeExecuteQuery();	
		query = "SELECT * FROM fasta_collect WHERE id_seq = ?";
		PreparedStatement queryExec = this.conn.prepareStatement(query);
		queryExec.setString(1, idSeqDNA);
		ResultSet results = queryExec.executeQuery();
		List<FastaContent> listFastaContent = new ArrayList<FastaContent>();
		while (results.next()){
			// id fasta_info
			/*
			 * Gerar arquivo de com Tempo de cada consulta
			 */
			String fileName = this.getFileNameFastaInfo(results.getInt(5));
			System.out.println("ID de Sequência encontrado no arquivo "+fileName);
			System.out.println("ID: "+results.getString(2));
			System.out.println("Sequência: "+results.getString(3));
			System.out.println("Linha: "+ results.getInt(4));
			FastaContent fastaInfo = new FastaContent(results.getString(2), results.getString(3), results.getInt(4));
			listFastaContent.add(fastaInfo);
		}
		if (listFastaContent.isEmpty()){
			System.out.println("*** ID "+idSeqDNA+" não encontrado no Banco de dados :(");
		}
		afterExecuteQuery();
		
		
	}
	
	public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

}
