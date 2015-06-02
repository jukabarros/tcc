package main;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;

import config.ReadProperties;
import create.CassandraCreateExperiment2;
import create.MongoDBCreate;
import create.MySQLCreate;
import dao.CassandraExperiment2DAO;
import dao.MongoDBDAO;
import dao.MySQLDAO;
import file.FastaReaderToCassandra;
import file.FastaReaderToMongoDB;
import file.FastaReaderToMySQL;

public class Application {
	
	/*
	 * Possiveis Argumentos:
	 * 0 - Arquivo ou Diretorio do fasta
	 * 1 - Descricao do arquivo do fasta
	 * 2 - Arquivo de Saida
	 */
	public static void main(String[] args) throws IOException, SQLException {
		Properties prop = ReadProperties.getProp();
		//String fastaDirectory = args[0]; 
		String fastaDirectory = prop.getProperty("fasta.directory");
		String bd = prop.getProperty("database").toUpperCase();
		String cleanData = prop.getProperty("clean.data").toUpperCase();
		long startTime = System.currentTimeMillis();
		if(bd.equals("CASSANDRA")){
			if(cleanData.equals("YES")){
				CassandraCreateExperiment2.main(null);
				FastaReaderToCassandra frToCassandra = new FastaReaderToCassandra();
				frToCassandra.readFastaDirectory(fastaDirectory);
			}else{
				CassandraExperiment2DAO dao = new CassandraExperiment2DAO();
				dao.findByID(">1305_150_799_F3");
			}
		
		}else if (bd.equals("MONGODB")){
			if(cleanData.equals("YES")){
				MongoDBCreate.main(null);
				FastaReaderToMongoDB frToMongo = new FastaReaderToMongoDB();
				frToMongo.readFastaDirectory(fastaDirectory);
			}else{
				MongoDBDAO dao = new MongoDBDAO();
				dao.findByID(">1303_37_58_F3");
			}
			
		}else if (bd.equals("MYSQL")){
			if(cleanData.equals("YES")){
				MySQLCreate.main(null);
				FastaReaderToMySQL frToMySQL = new FastaReaderToMySQL();
				frToMySQL.readFastaDirectory(fastaDirectory);
			}else{
				MySQLDAO dao = new MySQLDAO();
				dao.findByID(">1303_37_58_F3");
			}
			
		}  else{
			System.out.println("Opção de banco inválida :(");
		}

		long endTime = System.currentTimeMillis();

		long totalTime = endTime - startTime;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n******** Tempo de execução: " + formatter.format(totalTime / 1000d) + " segundos");
	}

}
