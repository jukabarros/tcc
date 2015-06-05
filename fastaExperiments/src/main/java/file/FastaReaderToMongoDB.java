package file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;

import config.ReadProperties;
import dao.MongoDBDAO;

public class FastaReaderToMongoDB {
	
	public int allLines;
	
	private MongoDBDAO dao;
	
	// Numero da linha de um arquivo especifico
	private int lineNumber;
	
	/* Sao usadas para criar o arquivo txt indicando
	 * o tempo de insercao de cada arquivo
	 */
	private File fileInsertTimeMongoDB;
	
	private FileWriter fwMongoDBInsertTime;
	
	private BufferedWriter bwMongoDBInsertTime;
	
	public FastaReaderToMongoDB() throws IOException {
		super();
		this.allLines = 0;
		this.lineNumber = 0;
		this.dao = new MongoDBDAO();
		
		this.fileInsertTimeMongoDB = null;
		this.fwMongoDBInsertTime = null;
		this.bwMongoDBInsertTime = null;
	}
	
	/**
	 * Ler todos os Fasta de um repositorio especifico
	 * Para cada fasta, sera criada uma tabela com o conteudo do arquivo 
	 * e sera registrado na tabela 'fasta_info' o nome do arquivo e suas caracteristicas
	 * @param fastaDirectory
	 * @throws IOException 
	 */
	public void readFastaDirectory(String fastaDirectory) throws IOException{
		File directory = new File(fastaDirectory);
		//get all the files from a directory
		File[] fList = directory.listFiles();
		
		// Criando o arquivo txt referente ao tempo de insercao no bd
		this.createInsertTimeTxt();
		
		for (File file : fList){
			if (file.isFile()){
				System.out.println("Lendo o arquivo: "+file.getName());
				if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
					System.out.println("*** Indexando o arquivo: "+file.getName());
					long sizeInMb = file.length() / (1024 * 1024);
					this.dao.insertFastaInfo(file.getName(), "Inserir comentario", sizeInMb);
					System.out.println("OK\n");
					this.dao.getCollection(file.getName());
					this.lineNumber = 0;
					
					long startTime = System.currentTimeMillis();
					this.readFastaFile(file.getAbsolutePath());
					long endTime = System.currentTimeMillis();
					
					// Calculando o tempo de insercao de cada arquivo
					String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
					this.bwMongoDBInsertTime.write(file.getName() + '\t' + timeExecutionSTR + '\n');
					
					System.out.println("** Fim da leitura do arquivo: "+file.getName());
				}else {
					System.out.println("*** Atenção: "+file.getName()+ " não é um arquivo .fasta");
				}
			}
		}
		this.bwMongoDBInsertTime.close();
		this.fwMongoDBInsertTime = null;
		this.fileInsertTimeMongoDB = null;
		
		System.out.println("**** Fim da Inserção no MongoDB.");
		System.out.println("**** Total de linhas inseridas no Banco: "+this.allLines/2);
	}
	/**
	 * Ler um fasta especifico e insere no MongoDB
	 * @param fastaFile
	 * @throws IOException 
	 */
	public void readFastaFile(String fastaFile) throws IOException{
		BufferedReader br = null;
		String line = "";
		String fastaSplitBy = "\n";
		Properties prop = ReadProperties.getProp();
		int srsSize = Integer.parseInt(prop.getProperty("srs.quantity"))*2;
		int numOfLine = 0;
		try {
			br = new BufferedReader(new FileReader(fastaFile));
			String idSeq = "";
			String seqDNA = "";
			System.out.println("**** Inserindo o arquivo fasta no MongoDB");
			
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
					this.dao.insertData(idSeq, seqDNA, this.lineNumber/2);
					idSeq = "";
					seqDNA = "";
				}
				// Printando a cada 100 000 registro inseridos
				if (this.lineNumber%200000 == 0){
					System.out.println("Quantidade de registros inseridos: "+this.lineNumber/2);
				}
			}
	 
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
		this.fileInsertTimeMongoDB = new File("mongoDBInsertTime.txt");
		this.fwMongoDBInsertTime = new FileWriter(this.fileInsertTimeMongoDB.getAbsoluteFile());
		this.bwMongoDBInsertTime = new BufferedWriter(this.fwMongoDBInsertTime);
		
		// if file doesnt exists, then create it
		if (!this.fileInsertTimeMongoDB.exists()) {
			this.fileInsertTimeMongoDB.createNewFile();
		}
		
	}
}
