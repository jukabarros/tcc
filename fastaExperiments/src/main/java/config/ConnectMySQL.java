package config;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class ConnectMySQL {
	
	private Connection conn;
	
	public Connection connectMysql(){
		try{
			Properties prop = ReadProperties.getProp();
			String host = prop.getProperty("mysql.host");
			String user = prop.getProperty("mysql.user");;
	        String password = prop.getProperty("mysql.password");
	        String database = prop.getProperty("mysql.db");
			
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://"+host+":3306/"+database, user, password);
			return conn;
		}catch (Exception e){
			System.out.println("Erro ao se conectar com o BD: "+e.getMessage());
		}
		return conn;
	}
	
	public static void  main(String args[]) throws IOException {
        
        ConnectMySQL con = new ConnectMySQL();
        con.connectMysql();
    }

}
