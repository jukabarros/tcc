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
	private File fileInsertTimeCassandra;
	
	private FileWriter fwCassandraInsertTime;
	
	private BufferedWriter bwCassandraInsertTime;
	
	
	public FastaReaderToCassandra() throws IOException {
		super();
		this.allLines = 0;
		this.dao = new CassandraDAO();
		
		this.fileInsertTimeCassandra = null;
		this.fwCassandraInsertTime = null;
		this.bwCassandraInsertTime = null;
	}
	
	/**
	 * Ler todos os Fasta de um repositorio especifico
	 * e insere as informacoes do arquivo na tabela fasta_info
	 * @param fastaDirectory
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public void readFastaDirectory(String fastaFilePath) throws SQLException, IOException{
		File directory = new File(fastaFilePath);
		//get all the files from a directory
		File[] fList = directory.listFiles();
		
		// Criando o arquivo txt referente ao tempo de insercao no bd
		this.createInsertTimeTxt();
		
		for (File file : fList){
			if (file.isFile()){
				System.out.println("** Lendo o arquivo: "+file.getName());
				if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
					double sizeInMb = file.length() / (1024 * 1024);
					System.out.println("* Indexando o arquivo "+file.getName());
					this.dao.insertFastaInfo(file.getName(), sizeInMb, "Inserir comentario");
					
					this.lineNumber = 0;
					long startTime = System.currentTimeMillis();
					this.readFastaFile(file.getAbsolutePath(), file.getName());
					long endTime = System.currentTimeMillis();
					
					// Calculando o tempo de insercao de cada arquivo
					String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
					this.bwCassandraInsertTime.write(file.getName() + '\t' + timeExecutionSTR + '\n');
					
					System.out.println("** Fim da leitura do arquivo: "+file.getName());
				}else {
					System.out.println("*** Atenção "+file.getName()+ " não é um arquivo fasta");
				}
			}
		}
		
		this.bwCassandraInsertTime.close();
		this.fwCassandraInsertTime = null;
		this.fileInsertTimeCassandra = null;
		
		System.out.println("\n**** Fim da Inserção no Cassandra.");
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
			System.out.println("**** Inserindo o arquivo no Cassandra");
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
					// Printando a cada 100 000 registro inseridos
					if (this.lineNumber%200000 == 0){
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
	private void createInsertTimeTxt() throws IOException{
		this.fileInsertTimeCassandra = new File("cassandraInsertTime.txt");
		this.fwCassandraInsertTime = new FileWriter(this.fileInsertTimeCassandra.getAbsoluteFile());
		this.bwCassandraInsertTime = new BufferedWriter(this.fwCassandraInsertTime);
		
		// if file doesnt exists, then create it
		if (!this.fileInsertTimeCassandra.exists()) {
			this.fileInsertTimeCassandra.createNewFile();
		}
		
	}

}
