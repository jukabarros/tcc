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
import dao.MySQLDAO;

public class FastaReaderToMySQL {
	
	// Ler todas as linhas (Soma com as linhas de outros arquivos)
	public int allLines;
	// Numero da linha de um arquivo especifico
	private int lineNumber;
	
	private MySQLDAO dao;
	
	/* Sao usadas para criar o arquivo txt indicando
	 * o tempo de insercao de cada arquivo
	 */
	private File fileInsertTimeMySQL;
	
	private FileWriter fwMySQLInsertTime;
	
	private BufferedWriter bwMySQLInsertTime;
	
	
	public FastaReaderToMySQL() {
		super();
		this.allLines = 0;
		this.lineNumber = 0;
		this.dao = new MySQLDAO();
		
		this.fileInsertTimeMySQL = null;
		this.fwMySQLInsertTime = null;
		this.bwMySQLInsertTime = null;
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
					long sizeInMb = file.length() / (1024 * 1024);
					this.dao.insertFastaInfo(file.getName(), sizeInMb, "Inserir comentario");
					// Recuperando id do arquivo para inserir na tabela fasta_collect
					int idFastaInfo = this.dao.getIDFastaInfo(file.getName());
					
					this.lineNumber = 0;
					long startTime = System.currentTimeMillis();
					this.readFastaFile(file.getAbsolutePath(), idFastaInfo);
					long endTime = System.currentTimeMillis();
					
					// Calculando o tempo de insercao de cada arquivo
					String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
					this.bwMySQLInsertTime.write(file.getName() + '\t' + timeExecutionSTR + '\n');
					
					System.out.println("** Fim da leitura do arquivo: "+file.getName());
				}else {
					System.out.println("*** Atenção "+file.getName()+ " não é um arquivo fasta");
				}
			}
		}
		
		this.bwMySQLInsertTime.close();
		this.fwMySQLInsertTime = null;
		this.fileInsertTimeMySQL = null;
		
		System.out.println("**** Fim da Inserção no MySQL.");
		System.out.println("**** Total de linhas inseridas no Banco: "+this.allLines/2);
	}
	/**
	 * Ler um Fasta especifico e insere no Cassandra
	 * @param fastaFile
	 * @throws SQLException 
	 */
	public void readFastaFile(String fastaFile, int idFastaInfo) throws SQLException{
		BufferedReader br = null;
		String line = "";
		String fastaSplitBy = "\n";
	 
		int numOfLine = 0;
		try {
			br = new BufferedReader(new FileReader(fastaFile));
			String idSeq = "";
			String seqDNA = "";
			System.out.println("**** Inserindo o arquivo fasta no MySQL");
			
			this.dao.beforeExecuteQuery();
			Properties prop = ReadProperties.getProp();
			int srsSize = Integer.parseInt(prop.getProperty("srs.quantity"))*2;
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
				if (numOfLine%srsSize == 0){
					this.dao.insertFastaCollect(idSeq, seqDNA, this.lineNumber/2, idFastaInfo);
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
		this.fileInsertTimeMySQL = new File("mySQLInsertTime.txt");
		this.fwMySQLInsertTime = new FileWriter(this.fileInsertTimeMySQL.getAbsoluteFile());
		this.bwMySQLInsertTime = new BufferedWriter(this.fwMySQLInsertTime);
		
		// if file doesnt exists, then create it
		if (!this.fileInsertTimeMySQL.exists()) {
			this.fileInsertTimeMySQL.createNewFile();
		}
		
	}
}
