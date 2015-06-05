package main;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import config.ReadProperties;
import create.CassandraCreate;
import create.MongoDBCreate;
import create.MySQLCreate;
import dao.CassandraDAO;
import dao.MongoDBDAO;
import dao.MySQLDAO;
import dna.FastaContent;
import file.FastaReaderToCassandra;
import file.FastaReaderToMongoDB;
import file.FastaReaderToMySQL;
import file.OutputFasta;

public class Application {
	
	public long calcTimeExecution (long start, long end){
		long totalTime = end - start;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n******** Tempo total de execução: " 
				+ formatter.format(totalTime / 1000d) + " segundos \n");
		
		return totalTime;
	}
	/*
	 * Possiveis Argumentos:
	 * 0 - Arquivo ou Diretorio do fasta
	 * 1 - Arquivo de Saida
	 */
	public static void main(String[] args) throws IOException, SQLException {
		Application app = new Application();
		String fastaDirectory = null;
		String fileNameOutput = null;

		Properties prop = ReadProperties.getProp();

		int numOfArgs = args.length;
		switch (numOfArgs) {
		case 0:
			System.out.println("** Capturando os parametros no arquivo properties");
			fastaDirectory = prop.getProperty("fasta.directory");
			fileNameOutput = prop.getProperty("file.name.output");
			break;
		case 1:
			fastaDirectory = args[0];
			fileNameOutput = prop.getProperty("file.name.output");
			break;
		default:
			fastaDirectory = args[0];
			fileNameOutput = args[1];
			break;
		}

		String bd = prop.getProperty("database").toUpperCase();
		String insertData = prop.getProperty("insert.data").toUpperCase();
		String idSeqDNA = prop.getProperty("id.seqDNA");
		String extractToFile = prop.getProperty("extract.file").toUpperCase();
		List<FastaContent> listFastaContent = new ArrayList<FastaContent>();
		long startTime = System.currentTimeMillis();
		/*
		 * INSERINDO / EXTRAINDO DO BD
		 */
		if(bd.equals("CASSANDRA")){
			if(insertData.equals("YES")){
				
				CassandraCreate.main(null);
				FastaReaderToCassandra frToCassandra = new FastaReaderToCassandra();
				frToCassandra.readFastaDirectory(fastaDirectory);
				
			}else{
				CassandraDAO dao = new CassandraDAO();
				if (idSeqDNA.equals("0")){
					dao.findAll();
				}else{
					dao.findByID(idSeqDNA);
				}
			}

		}else if (bd.equals("MONGODB")){
			if(insertData.equals("YES")){
				MongoDBCreate.main(null);
				FastaReaderToMongoDB frToMongo = new FastaReaderToMongoDB();
				frToMongo.readFastaDirectory(fastaDirectory);
			}else{
				MongoDBDAO dao = new MongoDBDAO();
				if (extractToFile.equals("YES")){
					System.out.println("\n**** Extraindo o conteudo: "+fileNameOutput);
					listFastaContent = dao.findByCollection(fileNameOutput);
				}else{
					dao.findByID(idSeqDNA);
				}
			}

		}else if (bd.equals("MYSQL")){
			if(insertData.equals("YES")){
				MySQLCreate.main(null);
				FastaReaderToMySQL frToMySQL = new FastaReaderToMySQL();
				frToMySQL.readFastaDirectory(fastaDirectory);
			}else{
				MySQLDAO dao = new MySQLDAO();
				if (extractToFile.equals("YES")){
					System.out.println("\n**** Extraindo o conteudo: "+fileNameOutput);
					listFastaContent = dao.findByFilename(fileNameOutput);
				}else{
					listFastaContent =  dao.findByID(idSeqDNA);
					System.out.println("\n**** Registro(s) encontrados com ID: "+idSeqDNA);
					for (int i = 0; i < listFastaContent.size(); i++) {
						System.out.println("\nID: "+listFastaContent.get(i).getId());
						System.out.println("Seq DNA: "+listFastaContent.get(i).getSeqDNA());
						System.out.println("Linha: "+listFastaContent.get(i).getLine());
					}
					
				}
			}

		}  else{
			System.out.println("Opção de banco inválida :(");
		}

		long endTime = System.currentTimeMillis();
		app.calcTimeExecution(startTime, endTime);

		/*
		 * CRIANDO O ARQUIVO DE SAIDA .fasta ou .fa
		 */
		String createOutputFile = prop.getProperty("create.output.file").toUpperCase();
		if (createOutputFile.equals("YES")){
			if (listFastaContent.isEmpty()){
				System.out.println("*** Não foi possível gerar o arquivo fasta.\n"
						+ "Para Gerar o arquivo fasta é necessário realizar a extração do Banco de Dados.\n"
						+ "OBS.: Caso a inserção já foi feita coloque o valor 'no' na propriedade 'insert.data'.");
			}else{

				System.out.println("*** Criando o arquivo: "+fileNameOutput);
				OutputFasta outputFasta = new OutputFasta();
				outputFasta.createFastaFile(fileNameOutput);
				outputFasta.prepareFastaFile(listFastaContent);
				outputFasta.closeFastaFile();
				System.out.println("*** Fim ***");
			}

		}
	}

}
