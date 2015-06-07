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
import create.CassandraCreate;
import dna.FastaContent;

public class CassandraDAO {
	
	private ConnectCassandra connCassandra;
	private Session session;
	private String query;
	private String keyspace;
	private CassandraCreate cassandraCreate;
	
	public CassandraDAO() throws IOException {
		super();
		this.connCassandra = new ConnectCassandra();
		this.session = null;
		this.query = null;
		Properties prop = ReadProperties.getProp();
		this.keyspace =  prop.getProperty("cassandra.keyspace");
		this.cassandraCreate = new CassandraCreate();
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
		List<FastaContent> listFastaInfo = new ArrayList<FastaContent>();
		for (Row row : results) {
//			System.out.println(String.format("%-30s\t%-70s", row.getString("id"), row.getString("seq_dna")));
			FastaContent fastaInfo = new FastaContent(row.getString("id"), row.getString("seq_dna"), row.getInt(3));
			listFastaInfo.add(fastaInfo);
			fastaInfo = null;
			line++;
		}
		this.afterExecuteQuery();
		System.out.println();
		System.out.println("******* Quantidade de linhas: "+line);
	}
	
	/**
	 * Insere o conteudo do arquivo fasta na tabela referente ao arquivo
	 * essa tabela possui o mesmo nome do arquivo
	 * @param table
	 * @param id
	 * @param seqDna
	 * @param line
	 */
	public void insertData(String table, String idSeq, String seqDna, int line){
		try{
			String tableName = table.replace(".", "___");
			this.query = "INSERT INTO "+tableName+" (id_seq, seq_dna, line) VALUES (?, ?, ?);";
			PreparedStatement statement = this.session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			this.session.execute(boundStatement.bind(idSeq, seqDna, line));
			
		}catch (Exception e){
			System.out.println("Erro ao executar a query: :("+e.getMessage());
		}
	}
	
	/**
	 * Insere as informacoes do arquivo fasta na tabela fasta_info, que vai servir
	 * como index para os arquivos que serão inseridos
	 * 
	 * @param fileName
	 * @param size
	 * @param comment
	 */
	public void insertFastaInfo(String fileName, double size, String comment){
		try{
			this.beforeExecuteQuery();
			this.query = "INSERT INTO fasta_info (file_name, size, comment) VALUES (?, ?, ?);";
			PreparedStatement statement = this.session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			this.session.execute(boundStatement.bind(fileName, size, comment));
			this.afterExecuteQuery();
			this.cassandraCreate.createTable(fileName);
		}catch (Exception e){
			System.out.println("Erro ao executar a query: :("+e.getMessage());
		}
	}
	
	public void findByFileName(String fileName){
		String tableName = fileName.replace("___", ".");
		this.beforeExecuteQuery();
		this.query = "SELECT * FROM fasta_info WHERE file_name = ?;";
		PreparedStatement statement = this.session.prepare(query);
		BoundStatement boundStatement = new BoundStatement(statement);
		ResultSet results = this.session.execute(boundStatement.bind(tableName));
		if (!results.all().isEmpty()){
			this.findByTable(tableName);
		}else{
			System.out.println("*** Conteúdo do arquivo não encontrado no Banco de dados :(");
		}
		this.afterExecuteQuery();
	}
	
	public void findByTable(String table){
		
		this.beforeExecuteQuery();
		this.query = "SELECT * FROM "+table;
		ResultSet results = this.session.execute(query);
		int line = 0;
		List<FastaContent> listFastaContent = new ArrayList<FastaContent>();
		for (Row row : results) {
			FastaContent fastaContent = new FastaContent(row.getString("id"), row.getString("seq_dna"), row.getInt("num_line"));
			listFastaContent.add(fastaContent);
			fastaContent = null;
			line++;
		}
		if (listFastaContent.isEmpty()){
			System.out.println("*** Conteúdo do arquivo não encontrado no Banco de dados :(");
		}
		this.afterExecuteQuery();
		System.out.println();
		System.out.println("**** Quantidade de linhas: "+line);
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
		List<FastaContent> listFastaInfo = new ArrayList<FastaContent>();
		for (Row row : results) {
//			System.out.println(String.format("%-30s\t%-70s", row.getString("id"), row.getString("seq_dna")));
			FastaContent fastaInfo = new FastaContent(row.getString("id"), row.getString("seq_dna"), row.getInt("num_line"));
			listFastaInfo.add(fastaInfo);
			fastaInfo = null;
			line++;
		}
		this.afterExecuteQuery();
		System.out.println();
		System.out.println("******* Quantidade de linhas: "+line);
	}

}
