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
import java.util.Arrays;
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
		idSequences.add(">557_2036_1480_F3"); //cabra4
		idSequences.add(">746_81_294_F3"); // cabra6
		idSequences.add(">560_29_216_F3"); // cabra5
		idSequences.add(">929_2036_1706_F3"); // cabra6
		idSequences.add(">932_36_394_F3"); // cabra7
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
		// Ordernando a lista por ordem alfabetica
		Arrays.sort(fList);
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
					System.out.println("* Inserindo o conteudo do arquivo no BD");
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
	 * Ler todos os Fasta de um repositorio especifico e realiza a consulta
	 * cada vez que um arquivo é inserido. É usado para fazer a curva de consulta
	 * @param fastaDirectory
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public void readFastaDirectoryAndSearch(String fastaFilePath, int repeat) throws SQLException, IOException{
		File directory = new File(fastaFilePath);
		//get all the files from a directory
		File[] fList = directory.listFiles();
		// Ordernando a lista por ordem alfabetica
		Arrays.sort(fList);
		
		List<String> idSequences = new ArrayList<String>();
		idSequences = this.addAllIdSeqs(idSequences);
		
		int paramConsult = 0;
		for (File file : fList){
			if (file.isFile()){
				System.out.println("** Lendo o arquivo: "+file.getName());
				if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
					
					this.createAnalistSearchTimeTxt(file.getName());
					this.bwMongoDB.write("****** CURVA DE CONSULTA ******\n");
					
					long sizeInMb = file.length() / (1024 * 1024);
					
					System.out.println("* Indexando o arquivo "+file.getName());
					this.dao.insertFastaInfo(file.getName(), "Inserir comentario", sizeInMb);
					
					System.out.println("* Inserindo o conteudo do arquivo no BD");
					this.dao.getCollection(file.getName());
					this.lineNumber = 0;
					this.readFastaFile(file.getAbsolutePath());
					
					System.out.println("\n\n** Iniciando as Consultas");
					// 5 -> Numero de amostra para o experimento
					for (int i = 0; i < 5; i++) {
						long startTime = System.currentTimeMillis();
						this.dao.findByID(idSequences.get(paramConsult));
						long endTime = System.currentTimeMillis();

						String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
						this.bwMongoDB.write(idSequences.get(paramConsult) + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
						
						paramConsult++;
					}
					this.bwMongoDB.close();
					this.fwMongoDB = null;
					this.fileTxtMongoDB = null;
					
					// Atualizando o numero de linhas inseridas no banco
					this.dao.updateNumOfLines(file.getName(), this.lineNumber/2);
					
				}else {
					System.out.println("*** Atenção "+file.getName()+ " não é um arquivo fasta");
				}
			}
		}
		
		System.out.println("\n\n\n********** FIM ************");
	}
	
	/**
	 * Metodo que adiciona os ids que serao consultados
	 * @param allIDSeq
	 * @return
	 */
	private List<String> addAllIdSeqs(List<String> allIDSeq){
		allIDSeq.add(">557_2036_1480_F3"); //cabra4
		allIDSeq.add(">746_81_294_F3"); // cabra6
		allIDSeq.add(">560_29_216_F3"); // cabra5
		allIDSeq.add(">929_2036_1706_F3"); // cabra6
		allIDSeq.add(">932_36_394_F3"); // cabra7
		allIDSeq.add(">1303_43_1188_F3");
		allIDSeq.add(">1303_346_24_F3");
		allIDSeq.add(">1303_42_1190_F3");
		allIDSeq.add(">929_1929_1490_F3");
		allIDSeq.add(">1303_43_80_F3");
		allIDSeq.add(">929_2039_1506_F3");
		allIDSeq.add(">746_142_260_F3");
		allIDSeq.add(">929_2039_1078_F3");
		allIDSeq.add(">1303_346_124_F3");
		allIDSeq.add(">1303_43_1458_F3");
		allIDSeq.add(">746_142_200_F3");
		allIDSeq.add(">929_2039_1280_F3");
		allIDSeq.add(">929_1929_1490_F3");
		allIDSeq.add(">929_2038_1875_F3");
		allIDSeq.add(">746_142_280_F3");
		allIDSeq.add(">1307_628_196_F3");
		allIDSeq.add(">1307_628_250_F3");
		allIDSeq.add(">746_142_175_F3");
		allIDSeq.add(">1307_628_256_F3");
		allIDSeq.add(">929_1929_1575_F3");
		allIDSeq.add(">1307_628_141_F3");
		return allIDSeq;
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
				// Printando a cada 500 000 registro inseridos
				if (this.lineNumber%1000000 == 0){
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
	
	/**
	 * Cria um arquivo txt que informa o tempo de consulta cada vez que um arquivo eh inserido
	 * Eh usado durante o experimento de curva de consulta.
	 * A escrita é feita no metodo que lista os diretorios
	 * 
	 * @param fastaFile
	 * @param timeExecution
	 * @throws IOException 
	 */
	private void createAnalistSearchTimeTxt(String fileName) throws IOException{
		this.fileTxtMongoDB = new File("analistSearch-"+fileName+"_mongoDB_.txt");
		this.fwMongoDB = new FileWriter(this.fileTxtMongoDB.getAbsoluteFile());
		this.bwMongoDB = new BufferedWriter(this.fwMongoDB);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtMongoDB.exists()) {
			this.fileTxtMongoDB.createNewFile();
		}
		
	}
}
