package create;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import config.ConnectMySQL;
import config.ReadProperties;

public class MySQLCreate {
	
	private String query;
	private String database;
	
	public MySQLCreate() throws IOException {
		this.query = null;
		Properties prop = ReadProperties.getProp();
		this.database = prop.getProperty("mysql.db"); 
	}
	
	public void createDatabase() throws SQLException{
		try{
			// Conexao vai vazio pois o banco ainda nao foi criado
			Connection conectar = new ConnectMySQL().connectMysql("");
			System.out.println("Creating Database "+this.database);
			this.query = "CREATE DATABASE IF NOT EXISTS "+this.database+";";
			PreparedStatement queryExec = conectar.prepareStatement(this.query);
			queryExec.execute();
			queryExec.close();
			System.out.println("OK");
			
		}catch(SQLException e){
			System.out.println("*** Erro ao criar o banco de dados: \n"+e.getMessage());
		}
	}
	
	public void dropDatabase() throws SQLException{
		try{
			Connection conectar = new ConnectMySQL().connectMysql("");
			System.out.println("Dropping Database "+this.database);
			this.query = "DROP DATABASE IF EXISTS "+this.database+";";
			PreparedStatement queryExec = conectar.prepareStatement(this.query);
			queryExec.execute();
			queryExec.close();
			System.out.println("OK");
		}catch(SQLException e){
			System.out.println("*** Erro ao deletar o banco de dados: \n "+e.getMessage());
		}
	}
	
	public void createTable() throws SQLException{
		try{
			Connection conectar = new ConnectMySQL().connectMysql(this.database);
			System.out.println("Creating table fasta_collect");
			this.query = "CREATE TABLE IF NOT EXISTS fasta_collect (id VARCHAR(50) PRIMARY KEY, seq_dna VARCHAR(52));";
			PreparedStatement queryExec = conectar.prepareStatement(this.query);
			queryExec.execute();
			queryExec.close();
			System.out.println("OK");
		}catch(SQLException e){
			System.out.println("*** Erro ao criar a tabela: \n "+e.getMessage());
		}
	}
	
	public void truncateTable() throws SQLException{
		try{
			Connection conectar = new ConnectMySQL().connectMysql(this.database);
			System.out.println("Cleaning table fasta_collect");
			this.query = "TRUNCATE fasta_collect;";
			PreparedStatement queryExec = conectar.prepareStatement(this.query);
			queryExec.execute();
			queryExec.close();
			System.out.println("OK");
		}catch(SQLException e){
			System.out.println("*** Erro ao limpar a tabela: \n "+e.getMessage());
		}
	}
	
	// Prepara o ambiente do MySQL
	public static void main(String[] args) throws SQLException, IOException {
		MySQLCreate mc = new MySQLCreate();
		
		mc.dropDatabase();
		
		mc.createDatabase();
		
		mc.createTable();

		mc.truncateTable();
	}

}
