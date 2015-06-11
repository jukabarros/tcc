package file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import config.ReadProperties;
import dao.CassandraDAO;

public class FastaReaderToCassandra {
	
	private int allLines;
	
	private CassandraDAO dao;
	
	// Numero da linha de um arquivo especifico
	private int lineNumber;
	
	/* Sao usadas para criar o arquivo txt indicando
	 * o tempo de insercao de cada arquivo
	 */
	private File fileTxtCassandra;
	
	private FileWriter fwCassandra;
	
	private BufferedWriter bwCassandra;
	
	private List<String> allFilesNames;
	
	public FastaReaderToCassandra() throws IOException {
		super();
		this.allLines = 0;
		this.dao = new CassandraDAO();
		
		this.fileTxtCassandra = null;
		this.fwCassandra = null;
		this.bwCassandra = null;
		this.allFilesNames = new ArrayList<String>();
	}
	
	/**
	 * Realiza todos os experimento de uma so vez, na seguinte ordem:
	 * Insere todos os arquivos fastas, consulta por 5 ids diferentes e extrai todos os arquivos
	 * Esse procedimento pode ser repetido de acordo com a propriedade 'num.repeat'
	 * @param fastaFilePath
	 * @param repeat
	 * @param srsSize
	 * @throws SQLException
	 * @throws IOException
	 */
	public void doAllExperiment(String fastaFilePath, int repeat, int srsSize) throws SQLException, IOException{
		System.out.println("\n\n** Iniciando a Inserção dos arquivos");
		this.readFastaDirectory(fastaFilePath, repeat, srsSize);
		
		List<String> idSequences = new ArrayList<String>();
		idSequences.add(">1303_40_1460_F3"); //cabra4
		idSequences.add(">1303_42_1190_F3"); // cabra6
		idSequences.add(">1303_37_58_F3"); // cabra5
		idSequences.add(">1303_43_50_F3"); // cabra6
		idSequences.add(">1303_38_874_F3"); // cabra7
		System.out.println("\n\n** Iniciando as Consultas dos arquivos");
		this.createConsultTimeTxt(repeat, srsSize);
		this.bwCassandra.write("****** CONSULTA ******\n");
		for (int i = 0; i < idSequences.size(); i++) {
			long startTime = System.currentTimeMillis();
			this.dao.findByID(idSequences.get(i));
			long endTime = System.currentTimeMillis();

			String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
			this.bwCassandra.write(idSequences.get(i) + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
		}
		this.bwCassandra.close();
		this.fwCassandra = null;
		this.fileTxtCassandra = null;
		
		System.out.println("\n\n** Iniciando a Extração dos arquivos");
		this.createExtractTimeTxt(repeat,srsSize);
		this.bwCassandra.write("****** EXTRAÇÃO ******\n");
		for (int i = 0; i < this.allFilesNames.size(); i++) {
			long startTime = System.currentTimeMillis();
			this.dao.findByFileName(this.allFilesNames.get(i), repeat);
			long endTime = System.currentTimeMillis();

			String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
			this.bwCassandra.write(this.allFilesNames.get(i) + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
		}
		this.bwCassandra.close();
		this.fwCassandra = null;
		this.fileTxtCassandra = null;
		
		System.out.println("\n\n\n********** FIM ************");
	}
	
	/**
	 * Ler todos os Fasta de um repositorio especifico
	 * e insere as informacoes do arquivo na tabela fasta_info
	 * @param fastaDirectory
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public void readFastaDirectory(String fastaFilePath, int repeat, int srsSize) throws SQLException, IOException{
		File directory = new File(fastaFilePath);
		//get all the files from a directory
		File[] fList = directory.listFiles();
		
		// Criando o arquivo txt referente ao tempo de insercao no bd
		this.createInsertTimeTxt(repeat, srsSize);
		this.bwCassandra.write("****** INSERÇÃO ******\n");
		for (File file : fList){
			if (file.isFile()){
				System.out.println("** Lendo o arquivo: "+file.getName());
				if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
					this.allFilesNames.add(file.getName());
					double sizeInMb = file.length() / (1024 * 1024);
					System.out.println("* Indexando o arquivo "+file.getName());
					this.dao.insertFastaInfo(file.getName(), sizeInMb, "Inserir comentario");
					System.out.println("* Inserindo o conteudo do arquivo no BD");
					this.lineNumber = 0;
					long startTime = System.currentTimeMillis();
					this.readFastaFile(file.getAbsolutePath(), file.getName());
					long endTime = System.currentTimeMillis();
					this.dao.updateNumOfLinesFastaInfo(file.getName(), lineNumber/2);
					// Calculando o tempo de insercao de cada arquivo
					String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
					this.bwCassandra.write(file.getName() + '\t' + timeExecutionSTR + '\n');
					
				}else {
					System.out.println("*** Atenção "+file.getName()+ " não é um arquivo fasta");
				}
			}
		}
		
		this.bwCassandra.close();
		this.fwCassandra = null;
		this.fileTxtCassandra = null;
		
		System.out.println("**** Total de linhas inseridas no Banco: "+this.allLines/2);
	}
	/**
	 * Ler um fasta especifico e insere no Cassandra
	 * @param fastaFile
	 * @throws IOException 
	 */
	public void readFastaFile(String fastaFile, String fastaFileName) throws IOException{
		BufferedReader br = null;
		String line = "";
		String fastaSplitBy = "\n";
		Properties prop = ReadProperties.getProp();
		int rssSize = Integer.parseInt(prop.getProperty("srs.quantity"))*2;
		int numOfLine = 0;
		try {
			br = new BufferedReader(new FileReader(fastaFile));
			String idSeq = "";
			String seqDNA = "";
			this.dao.beforeExecuteQuery();
			while ((line = br.readLine()) != null) {
				numOfLine++;
				this.allLines++;
				this.lineNumber++;
				String[] brokenFasta = line.split(fastaSplitBy);
				if (numOfLine%2 == 1){
					idSeq += brokenFasta[0];
				}else if (numOfLine > 1){
					seqDNA += brokenFasta[0];
				}
				if (numOfLine%rssSize == 0){
					this.dao.insertData(fastaFileName, idSeq, seqDNA, this.lineNumber/2);
					idSeq = "";
					seqDNA = "";
					// Printando a cada 500 000 registro inseridos
					if (this.lineNumber%1000000 == 0){
						System.out.println("Quantidade de registros inseridos: "+this.lineNumber/2);
					}
				}
			}
			this.dao.afterExecuteQuery();
	 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	  }
	
	private String calcTimeExecution (long start, long end){
		long totalTime = end - start;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n******** Tempo de execução: " 
				+ formatter.format(totalTime / 1000d) + " segundos \n");
		
		String totalTimeSTR = formatter.format(totalTime / 1000d)+ " segundos";
		return totalTimeSTR;
	}
	
	/**
	 * Cria um arquivo txt que informa o tempo de insercao de cada 
	 * arquivo Fasta
	 * A escrita é feita no metodo que lista os diretorios
	 * 
	 * @param fastaFile
	 * @param timeExecution
	 * @throws IOException 
	 */
	private void createInsertTimeTxt(int numOfRepeat, int srsSize) throws IOException{
		this.fileTxtCassandra = new File("test_"+numOfRepeat+"_cassandraInsertTime_SRS_"+srsSize+".txt");
		this.fwCassandra = new FileWriter(this.fileTxtCassandra.getAbsoluteFile());
		this.bwCassandra = new BufferedWriter(this.fwCassandra);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtCassandra.exists()) {
			this.fileTxtCassandra.createNewFile();
		}
		
	}
	
	/**
	 * Cria um arquivo txt que informa o tempo de extracao de cada 
	 * arquivo Fasta
	 * 
	 * @param fastaFile
	 * @param timeExecution
	 * @throws IOException 
	 */
	private void createExtractTimeTxt(int numOfRepeat, int srsSize) throws IOException{
		this.fileTxtCassandra = new File("test_"+numOfRepeat+"_cassandraExtractTime_SRS_"+srsSize+".txt");
		this.fwCassandra = new FileWriter(this.fileTxtCassandra.getAbsoluteFile());
		this.bwCassandra = new BufferedWriter(this.fwCassandra);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtCassandra.exists()) {
			this.fileTxtCassandra.createNewFile();
		}
		
	}
	
	/**
	 * Cria um arquivo txt que informa o tempo de consulta de cada 
	 * id de sequencia
	 * 
	 * @param fastaFile
	 * @param timeExecution
	 * @throws IOException 
	 */
	private void createConsultTimeTxt(int numOfRepeat, int srsSize) throws IOException{
		this.fileTxtCassandra = new File("test_"+numOfRepeat+"_cassandraConsultTime_SRS_"+srsSize+".txt");
		this.fwCassandra = new FileWriter(this.fileTxtCassandra.getAbsoluteFile());
		this.bwCassandra = new BufferedWriter(this.fwCassandra);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtCassandra.exists()) {
			this.fileTxtCassandra.createNewFile();
		}
		
	}

}
