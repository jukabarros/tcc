package main;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;

import config.ReadProperties;
import create.CassandraCreateExperiment2;
import create.MySQLCreate;
import dao.Experiment2DAO;
import file.FastaReaderCassandra;
import file.FastaReaderToMySQL;

public class Application {

	public static void main(String[] args) throws IOException, SQLException {
		Properties prop = ReadProperties.getProp();
		String fastaDirectory = prop.getProperty("fasta.directory");
		String bd = prop.getProperty("database");
		long startTime = System.currentTimeMillis();
		if(bd.equals("Cassandra")){
			CassandraCreateExperiment2.main(null);
			FastaReaderCassandra frToCassandra = new FastaReaderCassandra();
			frToCassandra.readFastaFile(fastaDirectory);
//			Experiment2DAO dao = new Experiment2DAO();
//			dao.findByID(">1305_150_799_F3");
//			dao.selectAll();
		}
		else if (bd.equals("MySQL")){
			MySQLCreate.main(null);
			FastaReaderToMySQL frToMySQL = new FastaReaderToMySQL();
			frToMySQL.readFastaDirectory(fastaDirectory);
		}
		
		long endTime = System.currentTimeMillis();
		
		long totalTime = endTime - startTime;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n******** TEMPO DE EXECUÇÃO: " + formatter.format(totalTime / 1000d) + " seconds");
	}

}
