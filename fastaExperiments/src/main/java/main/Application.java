package main;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


import config.ReadProperties;
import create.CassandraCreateExperiment2;
import create.MongoDBCreate;
import create.MySQLCreate;
import dao.CassandraExperiment2DAO;
import dao.MongoDBDAO;
import dao.MySQLDAO;
import dna.FastaInfo;
import file.FastaReaderToCassandra;
import file.FastaReaderToMongoDB;
import file.FastaReaderToMySQL;
import file.OutputFasta;

public class Application {

	/*
	 * Possiveis Argumentos:
	 * 0 - Arquivo ou Diretorio do fasta
	 * 1 - Descricao do arquivo do fasta
	 * 2 - Arquivo de Saida
	 */
	public static void main(String[] args) throws IOException, SQLException {
		String fastaDirectory = null;
		String outputFile = null;

		Properties prop = ReadProperties.getProp();

		int numOfArgs = args.length;
		switch (numOfArgs) {
		case 0:
			System.out.println("** Capturando os parametros no arquivo properties");
			fastaDirectory = prop.getProperty("fasta.directory");
			outputFile = prop.getProperty("output.file");
			break;
		case 1:
			fastaDirectory = args[0];
			outputFile = prop.getProperty("output.file");
			break;
		default:
			fastaDirectory = args[0];
			outputFile = args[1];
			break;
		}

		String bd = prop.getProperty("database").toUpperCase();
		String cleanData = prop.getProperty("clean.data").toUpperCase();
		String idSeqDNA = prop.getProperty("id.seqDna");
		List<FastaInfo> listFastaInfo = new ArrayList<FastaInfo>();
		long startTime = System.currentTimeMillis();

		if(bd.equals("CASSANDRA")){
			if(cleanData.equals("YES")){
				CassandraCreateExperiment2.main(null);
				FastaReaderToCassandra frToCassandra = new FastaReaderToCassandra();
				frToCassandra.readFastaDirectory(fastaDirectory);
			}else{
				CassandraExperiment2DAO dao = new CassandraExperiment2DAO();
				if (idSeqDNA.equals("0")){
					dao.findAll(outputFile);
				}else{
					dao.findByID(idSeqDNA);
				}
			}

		}else if (bd.equals("MONGODB")){
			if(cleanData.equals("YES")){
				MongoDBCreate.main(null);
				FastaReaderToMongoDB frToMongo = new FastaReaderToMongoDB();
				frToMongo.readFastaDirectory(fastaDirectory);
			}else{
				MongoDBDAO dao = new MongoDBDAO();
				if (idSeqDNA.equals("0")){
					dao.findAll(outputFile);
				}else{
					dao.findByID(idSeqDNA);
				}
			}

		}else if (bd.equals("MYSQL")){
			if(cleanData.equals("YES")){
				MySQLCreate.main(null);
				FastaReaderToMySQL frToMySQL = new FastaReaderToMySQL();
				frToMySQL.readFastaDirectory(fastaDirectory);
			}else{
				MySQLDAO dao = new MySQLDAO();
				if (idSeqDNA.equals("0")){
					listFastaInfo = dao.findAll();
				}else{
					dao.findByID(idSeqDNA);
				}
			}

		}  else{
			System.out.println("Opção de banco inválida :(");
		}

		long endTime = System.currentTimeMillis();

		long totalTime = endTime - startTime;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n******** Tempo de execução: " 
				+ formatter.format(totalTime / 1000d) + " segundos \n");

		String createOutputFile = prop.getProperty("create.output.file").toUpperCase();
		if (createOutputFile.equals("YES")){
			if (listFastaInfo.isEmpty()){
				System.out.println("*** É necessário realizar a extração.\n"
						+ " Coloque o valor 'no' na propriedade 'clean.data' se a inserção já foi feita");
			}else{

				System.out.println("*** Criando o arquivo: "+outputFile);
				OutputFasta outputFasta = new OutputFasta();
				outputFasta.createFastaFile(outputFile);
				outputFasta.prepareFastaFile(listFastaInfo);
				outputFasta.closeFastaFile();
				System.out.println("*** Fim ***");
			}

		}
	}

}
