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
	private File fileTxtMySQL;
	
	private FileWriter fwMySQL;
	
	private BufferedWriter bwMySQL;
	
	private List<String> allFilesNames;
	
	public FastaReaderToMySQL() throws IOException {
		super();
		this.allLines = 0;
		this.lineNumber = 0;
		this.dao = new MySQLDAO();
		
		this.fileTxtMySQL = null;
		this.fwMySQL = null;
		this.bwMySQL = null;
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
		this.bwMySQL.write("****** CONSULTA ******\n");
		for (int i = 0; i < idSequences.size(); i++) {
			long startTime = System.currentTimeMillis();
			this.dao.findByID(idSequences.get(i));
			long endTime = System.currentTimeMillis();

			String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
			this.bwMySQL.write(idSequences.get(i) + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
		}
		this.bwMySQL.close();
		this.fwMySQL = null;
		this.fileTxtMySQL = null;
		
		System.out.println("\n\n** Iniciando a Extração dos arquivos");
		this.createExtractTimeTxt(repeat,srsSize);
		this.bwMySQL.write("****** EXTRAÇÃO ******\n");
		for (int i = 0; i < this.allFilesNames.size(); i++) {
			long startTime = System.currentTimeMillis();
			this.dao.findByFilename(this.allFilesNames.get(i), repeat);
			long endTime = System.currentTimeMillis();

			String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
			this.bwMySQL.write(this.allFilesNames.get(i) + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
		}
		this.bwMySQL.close();
		this.fwMySQL = null;
		this.fileTxtMySQL = null;
		
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
		this.createInsertTimeTxt(repeat,srsSize);
		this.bwMySQL.write("****** INSERÇÃO ******\n");
		for (File file : fList){
			if (file.isFile()){
				System.out.println("** Lendo o arquivo: "+file.getName());
				if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
					long sizeInMb = file.length() / (1024 * 1024);
					
					// Adicionando os nomes dos arquivos na lista para extracao em doAllExperiments
					this.allFilesNames.add(file.getName());
					
					this.dao.insertFastaInfo(file.getName(), sizeInMb, "Inserir comentario");
					// Recuperando id do arquivo para inserir na tabela fasta_collect
					int idFastaInfo = this.dao.getIDFastaInfo(file.getName());
					
					this.lineNumber = 0;
					long startTime = System.currentTimeMillis();
					this.readFastaFile(file.getAbsolutePath(), idFastaInfo);
					long endTime = System.currentTimeMillis();
					
					// Atualizando o numero de linhas no arquivo
					this.dao.updateNumOfLinesFastaInfo(file.getName(), this.lineNumber/2);
					
					// Calculando o tempo de insercao de cada arquivo
					String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
					this.bwMySQL.write(file.getName() + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
					
				}else {
					System.out.println("*** Atenção "+file.getName()+ " não é um arquivo fasta");
				}
			}
		}
		
		this.bwMySQL.close();
		this.fwMySQL = null;
		this.fileTxtMySQL = null;
		
		System.out.println("\n**** Fim da Inserção no MySQL.");
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
					// Printando a cada 1 000 000 registro inseridos
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
		this.fileTxtMySQL = new File("test_"+numOfRepeat+"_mySQLInsertTime_SRS_"+srsSize+".txt");
		this.fwMySQL = new FileWriter(this.fileTxtMySQL.getAbsoluteFile());
		this.bwMySQL = new BufferedWriter(this.fwMySQL);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtMySQL.exists()) {
			this.fileTxtMySQL.createNewFile();
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
		this.fileTxtMySQL = new File("test_"+numOfRepeat+"_mySQLExtractTime_SRS_"+srsSize+".txt");
		this.fwMySQL = new FileWriter(this.fileTxtMySQL.getAbsoluteFile());
		this.bwMySQL = new BufferedWriter(this.fwMySQL);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtMySQL.exists()) {
			this.fileTxtMySQL.createNewFile();
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
		this.fileTxtMySQL = new File("test_"+numOfRepeat+"_mySQLConsultTime_SRS_"+srsSize+".txt");
		this.fwMySQL = new FileWriter(this.fileTxtMySQL.getAbsoluteFile());
		this.bwMySQL = new BufferedWriter(this.fwMySQL);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtMySQL.exists()) {
			this.fileTxtMySQL.createNewFile();
		}
		
	}
}
