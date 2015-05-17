package create;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.datastax.driver.core.Session;

import config.ConnectCassandra;


/*
 * Cria um keyspace com uma tabela que possui uma unica
 * coluna que vai ser inserida o ID com a sequencia
 */
public class CreateExperiment2 {
	
	private ConnectCassandra connCassandra;
	private Session session;
	private String query;
	
	public CreateExperiment2() {
		super();
		this.connCassandra = new ConnectCassandra();
		this.session = null;
		this.query = null;
	}
	
	public void createKeyspace(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		System.out.println("Creating keyspace fastaExperiment2...");
		try{
			this.query = "CREATE KEYSPACE fastaExperiment2 WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao criar o keyspace: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public void createTables(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		System.out.println("Creating tables...");
		try{
			this.query = "CREATE TABLE fastaExperiment2.fastaCollect (id text PRIMARY KEY, seq_dna text)";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao criar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public void dropKeyspace(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		System.out.println("Dropping Keyspace fastaExperiment2");
		try{
			this.query = "DROP KEYSPACE fastaExperiment2";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao limpar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public void truncateTable(String table){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		try{
			this.query = "TRUNCATE "+table;
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao limpar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		CreateExperiment2 create = new CreateExperiment2();
		create.dropKeyspace();
		System.out.println("OK");
		create.createKeyspace();
		System.out.println("OK");
		create.createTables();
		System.out.println("OK");
		long endTime = System.currentTimeMillis();
		
		long totalTime = endTime - startTime;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("******** EXECUTION TIME: " + formatter.format(totalTime / 1000d) + " seconds");

	}

}
