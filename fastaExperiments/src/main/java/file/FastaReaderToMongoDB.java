package file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import config.ReadProperties;
import dao.MongoDBDAO;

public class FastaReaderToMongoDB {
	
	public int lines;
	
	private MongoDBDAO dao;
	
	
	public FastaReaderToMongoDB() throws IOException {
		super();
		this.lines = 0;
		this.dao = new MongoDBDAO();
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
		for (File file : fList){
			if (file.isFile()){
				System.out.println("Lendo o arquivo: "+file.getName());
				if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
					this.readFastaFile(file.getAbsolutePath());
					System.out.println("** Fim da leitura do arquivo: "+file.getName());
				}else {
					System.out.println("*** Atenção: "+file.getName()+ " não é um arquivo .fasta");
				}
			}
		}
	}
	/**
	 * Ler um fasta especifico e insere no Cassandra
	 * @param fastaFile
	 * @throws IOException 
	 */
	public void readFastaFile(String fastaFile) throws IOException{
		BufferedReader br = null;
		String line = "";
		String fastaSplitBy = "\n";
		Properties prop = ReadProperties.getProp();
		int rssSize = Integer.parseInt(prop.getProperty("srr.quantity"))*2;
		int numOfLine = 0;
		try {
			br = new BufferedReader(new FileReader(fastaFile));
			String id = "";
			String seqDNA = "";
			System.out.println("**** Processando o arquivo fasta");
			while ((line = br.readLine()) != null) {
				numOfLine++;
				this.lines++;
				String[] brokenFasta = line.split(fastaSplitBy);
				if (numOfLine%2 == 1){
					id += brokenFasta[0];
				}else if (numOfLine > 1){
					seqDNA += brokenFasta[0];
				}
				if (numOfLine%rssSize == 0){
					this.dao.insertData(id, seqDNA);
					id = "";
					seqDNA = "";
				}
				if (this.lines%2000==0){
					System.out.println("*** Número de registros inseridos: "+this.lines/2);
				}
			}
			System.out.println("\n*** Número Total de registros inseridos: "+this.lines/2);
	 
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

}
