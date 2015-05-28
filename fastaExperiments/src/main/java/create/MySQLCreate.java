package create;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import config.ConnectMySQL;

public class MySQLCreate {
	
	private String query;
	
	public MySQLCreate() {
		this.query = null;
	}

	public void createTable() throws SQLException{
		Connection conectar = new ConnectMySQL().connectMysql();
		System.out.println("Creating table fasta_collect");
		query = "CREATE TABLE fasta_collect (id VARCHAR(50) PRIMARY KEY, seq_dna VARCHAR(52));";
		PreparedStatement queryExec = conectar.prepareStatement(query);
		queryExec.execute();
		queryExec.close();
	}
	
	public void truncateTable() throws SQLException{
		Connection conectar = new ConnectMySQL().connectMysql();
		System.out.println("Truncate table fasta_collect");
		query = "TRUNCATE fasta_collect;";
		PreparedStatement queryExec = conectar.prepareStatement(query);
		queryExec.execute();
		queryExec.close();
	}
	
	public static void main(String[] args) throws SQLException {
		MySQLCreate mc = new MySQLCreate();
		mc.truncateTable();
		System.out.println("OK");

	}

}
