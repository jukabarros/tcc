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
import dao.MongoDBDAO;

public class FastaReaderToMongoDB {
	
	public int allLines;
	
	private MongoDBDAO dao;
	
	// Numero da linha de um arquivo especifico
	private int lineNumber;
	
	/* Sao usadas para criar o arquivo txt indicando
	 * o tempo de insercao de cada arquivo
	 */
	private File fileTxtMongoDB;
	
	private FileWriter fwMongoDB;
	
	private BufferedWriter bwMongoDB;
	
	private List<String> allFilesNames;
	
	public FastaReaderToMongoDB() throws IOException {
		super();
		this.allLines = 0;
		this.lineNumber = 0;
		this.dao = new MongoDBDAO();
		
		this.fileTxtMongoDB = null;
		this.fwMongoDB = null;
		this.bwMongoDB = null;
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
		this.bwMongoDB.write("****** CONSULTA ******\n");
		for (int i = 0; i < idSequences.size(); i++) {
			long startTime = System.currentTimeMillis();
			this.dao.findByID(idSequences.get(i));
			long endTime = System.currentTimeMillis();

			String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
			this.bwMongoDB.write(idSequences.get(i) + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
		}
		this.bwMongoDB.close();
		this.fwMongoDB = null;
		this.fileTxtMongoDB = null;
		
		System.out.println("\n\n** Iniciando a Extração dos arquivos");
		this.createExtractTimeTxt(repeat,srsSize);
		this.bwMongoDB.write("****** EXTRAÇÃO ******\n");
		for (int i = 0; i < this.allFilesNames.size(); i++) {
			long startTime = System.currentTimeMillis();
			this.dao.findByCollection(this.allFilesNames.get(i), repeat);
			long endTime = System.currentTimeMillis();

			String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
			this.bwMongoDB.write(this.allFilesNames.get(i) + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
		}
		this.bwMongoDB.close();
		this.fwMongoDB = null;
		this.fileTxtMongoDB = null;
		
		System.out.println("\n\n\n********** FIM ************");
	}
	
	/**
	 * Ler todos os Fasta de um repositorio especifico
	 * Para cada fasta, sera criada uma tabela com o conteudo do arquivo 
	 * e sera registrado na tabela 'fasta_info' o nome do arquivo e suas caracteristicas
	 * @param fastaDirectory
	 * @throws IOException 
	 */
	public void readFastaDirectory(String fastaDirectory, int numOfRepeat, int srsSize) throws IOException{
		File directory = new File(fastaDirectory);
		//get all the files from a directory
		File[] fList = directory.listFiles();
		
		// Criando o arquivo txt referente ao tempo de insercao no bd
		this.createInsertTimeTxt(numOfRepeat, srsSize);
		this.bwMongoDB.write("****** INSERÇÃO ******\n");
		for (File file : fList){
			if (file.isFile()){
				System.out.println("Lendo o arquivo: "+file.getName());
				if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
					// Adicionando os arquivos para fazer a extracao em doAllExperiments
					this.allFilesNames.add(file.getName());
					System.out.println("*** Indexando o arquivo: "+file.getName());
					long sizeInMb = file.length() / (1024 * 1024);
					this.dao.insertFastaInfo(file.getName(), "Inserir comentario", sizeInMb);
					System.out.println("OK\n");
					this.dao.getCollection(file.getName());
					this.lineNumber = 0;
					
					long startTime = System.currentTimeMillis();
					this.readFastaFile(file.getAbsolutePath());
					long endTime = System.currentTimeMillis();
					
					// Atualizando o numero de linhas inseridas no banco
					this.dao.updateNumOfLines(file.getName(), this.lineNumber/2);
					
					// Calculando o tempo de insercao de cada arquivo
					String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
					this.bwMongoDB.write(file.getName() + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
					
					System.out.println("** Fim da leitura do arquivo: "+file.getName());
				}else {
					System.out.println("*** Atenção: "+file.getName()+ " não é um arquivo .fasta");
				}
			}
		}
		this.bwMongoDB.close();
		this.fwMongoDB = null;
		this.fileTxtMongoDB = null;
		
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
	private void createInsertTimeTxt(int numOfRepeat, int srsSize) throws IOException{
		this.fileTxtMongoDB = new File("test_"+numOfRepeat+"-mongoDBInsertTime_SRS_"+srsSize+".txt");
		this.fwMongoDB = new FileWriter(this.fileTxtMongoDB.getAbsoluteFile());
		this.bwMongoDB = new BufferedWriter(this.fwMongoDB);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtMongoDB.exists()) {
			this.fileTxtMongoDB.createNewFile();
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
		this.fileTxtMongoDB = new File("test_"+numOfRepeat+"_mongoDBExtractTime_SRS_"+srsSize+".txt");
		this.fwMongoDB = new FileWriter(this.fileTxtMongoDB.getAbsoluteFile());
		this.bwMongoDB = new BufferedWriter(this.fwMongoDB);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtMongoDB.exists()) {
			this.fileTxtMongoDB.createNewFile();
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
		this.fileTxtMongoDB = new File("test_"+numOfRepeat+"_mongoDBConsultTime_SRS_"+srsSize+".txt");
		this.fwMongoDB = new FileWriter(this.fileTxtMongoDB.getAbsoluteFile());
		this.bwMongoDB = new BufferedWriter(this.fwMongoDB);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtMongoDB.exists()) {
			this.fileTxtMongoDB.createNewFile();
		}
		
	}
}
