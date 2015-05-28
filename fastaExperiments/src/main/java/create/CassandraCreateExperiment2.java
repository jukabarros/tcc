package create;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;

import com.datastax.driver.core.Session;

import config.ConnectCassandra;
import config.ReadProperties;


/*
 * Cria um keyspace com uma tabela que possui uma unica
 * coluna que vai ser inserida o ID com a sequencia
 */
public class CassandraCreateExperiment2 {
	
	private ConnectCassandra connCassandra;
	private Session session;
	private String query;
	
	private String keyspace;
	private String replicationFactor;
	
	
	public CassandraCreateExperiment2() throws IOException {
		super();
		Properties prop = ReadProperties.getProp();
		this.connCassandra = new ConnectCassandra();
		this.session = null;
		this.query = null;
		this.keyspace =  prop.getProperty("cassandra.keyspace");
		this.replicationFactor = prop.getProperty("cassandra.replication.factor");
		
	}
	
	public void createKeyspace(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		try{
			System.out.println("Creating keyspace "+this.keyspace);
			this.query = "CREATE KEYSPACE IF NOT EXISTS "+this.keyspace+" WITH replication = {'class':'SimpleStrategy',"
					+ " 'replication_factor':"+this.replicationFactor+"};";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao criar o keyspace: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	/*
	 * Tabela que recebera todas informacoes do arquivo fasta,
	 * sera usada para apontar a tabela que contem todo conteudo do arquivo
	 */
	public void createTableFastaInfo(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		System.out.println("Creating table fasta_info");
		try{
			this.query = "CREATE TABLE IF NOT EXISTS "+this.keyspace+".fastaInfo (fasta_name text PRIMARY KEY, num_line bigint)";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao criar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public void createTables(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		System.out.println("Creating tables...");
		try{
			this.query = "CREATE TABLE IF NOT EXISTS "+this.keyspace+".fastaCollect "
					+ "(id text PRIMARY KEY, seq_dna text)";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao criar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public void dropKeyspace(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		System.out.println("Dropping Keyspace "+this.keyspace);
		try{
			this.query = "DROP KEYSPACE IF EXISTS "+this.keyspace+"";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao deletar o keyspace: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public void truncateTable(String table){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		try{
			System.out.println("Cleaning table: "+table);
			this.query = "TRUNCATE "+this.keyspace+"."+table;
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao limpar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}

	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();
		CassandraCreateExperiment2 create = new CassandraCreateExperiment2();
		create.truncateTable("fastaCollect");
		System.out.println("OK");
		create.dropKeyspace();
		System.out.println("OK");
		create.createKeyspace();
//		System.out.println("OK");
//		create.createTableFastaInfo();
		System.out.println("OK");
		create.createTables();
		System.out.println("OK");
		long endTime = System.currentTimeMillis();
		
		long totalTime = endTime - startTime;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("******** EXECUTION TIME: " + formatter.format(totalTime / 1000d) + " segundos\n");

	}

}
