package file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import config.ReadProperties;
import dao.CassandraDAO;

public class FastaReaderToCassandra {
	
	public int lines;
	
	private CassandraDAO dao;
	
	
	public FastaReaderToCassandra() throws IOException {
		super();
		this.lines = 0;
		this.dao = new CassandraDAO();
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
			List<String> allQuery = new ArrayList<String>();
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
					String query = "INSERT INTO fastaCollect (id, seq_dna, num_line) VALUES ('"+id+"', '"+seqDNA+"', "+this.lines/2+");";
					allQuery.add(query);
					System.out.println("IDs: "+id);
					id = "";
					seqDNA = "";
					break;
				}
			}
			System.out.println("**** Inserindo no Cassandra...");
			this.insertAllData(allQuery);
	 
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
	
	private void insertAllData(List<String> allData){
		try{
			this.dao.beforeExecuteQuery();
			for (int i = 0; i < allData.size(); i++) {
				this.dao.executeQuery(allData.get(i));
				if (i%1000 == 0){
					System.out.println("Numero de registros inseridos: "+i);
				}
			}
			this.dao.afterExecuteQuery();
			System.out.println("**** Total de linhas inseridas no Banco: "+this.lines/2);
		}catch (Exception e){
			System.out.println("Erro ao executar a query :( \n"+e.getMessage());
		}
	}

}
