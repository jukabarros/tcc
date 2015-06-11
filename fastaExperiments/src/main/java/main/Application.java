package main;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;

import config.ReadProperties;
import create.CassandraCreate;
import create.MongoDBCreate;
import create.MySQLCreate;
import dao.CassandraDAO;
import dao.MongoDBDAO;
import dao.MySQLDAO;
import file.FastaReaderToCassandra;
import file.FastaReaderToMongoDB;
import file.FastaReaderToMySQL;

public class Application {
	
	public long calcTimeExecution (long start, long end){
		long totalTime = end - start;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n******** Tempo total de execução: " 
				+ formatter.format(totalTime / 1000d) + " segundos \n");
		
		return totalTime;
	}
	/*
	 * Argumentos Opcionais:
	 * 0 - Arquivo ou Diretorio do fasta
	 * 1 - Tamanho da SRS
	 * 
	 * Automatizar todo experimento na seguinte ordem:
	 * Inserir todos os fastas, Consultar por 5 IDs de Sequencias ja definidos e Extrair todos os arquivos
	 * Para cada situação Gerar um arquivo TXTs
	 */
	public static void main(String[] args) throws IOException, SQLException {
		Application app = new Application();
		String fastaDirectory = null;
		String fileNameOutput = null;
		int srsSize = 0;
		Properties prop = ReadProperties.getProp();
		int numOfRepeat = Integer.parseInt(prop.getProperty("num.repeat"));
		int numOfArgs = args.length;
		switch (numOfArgs) {
		case 0:
			System.out.println("** Capturando os parametros no arquivo properties");
			fastaDirectory = prop.getProperty("fasta.directory");
			srsSize = Integer.parseInt(prop.getProperty("srs.quantity"));
			break;
		case 1:
			fastaDirectory = args[0];
			srsSize = Integer.parseInt(prop.getProperty("srs.quantity"));
			break;
		default:
			fastaDirectory = args[0];
			srsSize = Integer.parseInt(args[1]);
			break;
		}
		String allExperiment = prop.getProperty("all.experiments").toUpperCase();
		System.out.println("* Executar todos experimentos: "+allExperiment);
		System.out.println("* Numero de Repeticões: "+numOfRepeat);
		
		System.out.println("* Tamanho da SRS: "+srsSize);
		String db = prop.getProperty("database").toUpperCase();
		System.out.println("* Banco de Dados: "+db);
		
		String insertData = prop.getProperty("insert.data").toUpperCase();
		
		String idSeqDNA = prop.getProperty("id.seqDNA");
		String extractData = prop.getProperty("extract.data").toUpperCase();
		
		long startTime = System.currentTimeMillis();
		/*
		 * INSERINDO / EXTRAINDO DO BD
		 */
		if(db.equals("CASSANDRA")){
			if(insertData.equals("YES") ||  allExperiment.equals("YES")){
				for (int i = 1; i <= numOfRepeat; i++) {
					CassandraCreate.main(null);
					FastaReaderToCassandra frToCassandra = new FastaReaderToCassandra();
					frToCassandra.readFastaDirectory(fastaDirectory, i, srsSize);
				}
				
			}else{
				CassandraDAO dao = new CassandraDAO();
				if (extractData.equals("YES")){
					System.out.println("\n**** Extraindo o conteudo de "+fileNameOutput);
					dao.findByFileName(fileNameOutput);
				}else{
					System.out.println("\n**** Consultando por id de sequencia: "+idSeqDNA);
					dao.findByID(idSeqDNA);
				}
			}

		}else if (db.equals("MONGODB")){
			if (allExperiment.equals("YES")){
				for (int i = 1; i <= numOfRepeat; i++) {
					System.out.println("***************** Repetição: "+i);
					MongoDBCreate.main(null);
					FastaReaderToMongoDB frToMongo = new FastaReaderToMongoDB();
					frToMongo.doAllExperiment(fastaDirectory, i, srsSize);
				}
				
			}
			else if(insertData.equals("YES")){
				MongoDBCreate.main(null);
				FastaReaderToMongoDB frToMongo = new FastaReaderToMongoDB();
				frToMongo.readFastaDirectory(fastaDirectory, 0, srsSize);

			}else{
				MongoDBDAO dao = new MongoDBDAO();
				if (extractData.equals("YES")){
					System.out.println("\n**** Extraindo o conteudo de "+fileNameOutput);
					dao.findByCollection(fileNameOutput, 0);
				}else{
					System.out.println("\n**** Consultando por id de sequencia: "+idSeqDNA);
					dao.findByID(idSeqDNA);
				}
			}

		}else if (db.equals("MYSQL")){
			if (allExperiment.equals("YES")){
				for (int i = 1; i <= numOfRepeat; i++) {
					System.out.println("***************** Repetição: "+i);
					MySQLCreate.main(null);
					FastaReaderToMySQL frToMySQL = new FastaReaderToMySQL();
					frToMySQL.doAllExperiment(fastaDirectory, i, srsSize);
				}
				
			}
			else if(insertData.equals("YES")){
				MySQLCreate.main(null);
				FastaReaderToMySQL frToMySQL = new FastaReaderToMySQL();
				frToMySQL.readFastaDirectory(fastaDirectory, 1, srsSize);

			}else{
				MySQLDAO dao = new MySQLDAO();
				if (extractData.equals("YES")){
					System.out.println("\n**** Extraindo o conteudo de "+fileNameOutput);
					dao.findByFilename(fileNameOutput, 0);
				}else{
					System.out.println("\n**** Consultando por id de sequencia: "+idSeqDNA);
					dao.findByID(idSeqDNA);
				}
			}

		}  else{
			System.out.println("Opção de banco inválida :(");
		}

		long endTime = System.currentTimeMillis();
		app.calcTimeExecution(startTime, endTime);
		
	}

}
