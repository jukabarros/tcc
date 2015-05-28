package main;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;

import config.ReadProperties;
import create.CassandraCreateExperiment2;
import create.MySQLCreate;
import file.FastaReaderCassandra;
import file.FastaReaderToMySQL;

public class Application {
	
	/*
	 * Argumentos:
	 * 0 - Arquivo ou Diretorio do fasta
	 * 1 - Descricao do arquivo do fasta
	 * 2 - Insercao, extracao ou consulta
	 * 3 - Arquivo de Saida
	 */
	public static void main(String[] args) throws IOException, SQLException {
		Properties prop = ReadProperties.getProp();
		//String fastaDirectory = args[0]; 
		String fastaDirectory = prop.getProperty("fasta.directory");
		String bd = prop.getProperty("database").toUpperCase();
		long startTime = System.currentTimeMillis();
		if(bd.equals("CASSANDRA")){
			CassandraCreateExperiment2.main(null);
			FastaReaderCassandra frToCassandra = new FastaReaderCassandra();
			frToCassandra.readFastaFile(fastaDirectory);
			//			Experiment2DAO dao = new Experiment2DAO();
			//			dao.findByID(">1305_150_799_F3");
			//			dao.selectAll();
		
		}else if (bd.equals("MONGODB")){
			System.out.println("Vc escolheu MongoDB :D");

		}else if (bd.equals("MYSQL")){
			MySQLCreate.main(null);
			FastaReaderToMySQL frToMySQL = new FastaReaderToMySQL();
			frToMySQL.readFastaDirectory(fastaDirectory);
		}  else{
			System.out.println("Opção de banco inválida :(");
		}

		long endTime = System.currentTimeMillis();

		long totalTime = endTime - startTime;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n******** Tempo de execução: " + formatter.format(totalTime / 1000d) + " segundos");
	}

}
