package file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import dao.MySQLDAO;

public class FastaReaderToMySQL {
	
	// Ler todas as linhas (Soma com as linhas de outros arquivos)
	public int allLines;
	// Numero da linha de um arquivo especifico
	private int lineNumber;
	
	private MySQLDAO dao;
	
	
	public FastaReaderToMySQL() {
		super();
		this.allLines = 0;
		this.lineNumber = 0;
		this.dao = new MySQLDAO();
	}
	
	/**
	 * Ler todos os Fasta de um repositorio especifico
	 * e insere as informacoes do arquivo na tabela fasta_info
	 * @param fastaDirectory
	 * @throws SQLException 
	 */
	public void readFastaDirectory(String fastaDirectory) throws SQLException{
		File directory = new File(fastaDirectory);
		//get all the files from a directory
		File[] fList = directory.listFiles();
		
		for (File file : fList){
			if (file.isFile()){
				System.out.println("** Lendo o arquivo: "+file.getName());
				if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
					long sizeInMb = file.length() / (1024 * 1024);
					this.dao.insertFastaInfo(file.getName(), sizeInMb, "Inserir comentario");
					// Recuperando id do arquivo para inserir na tabela fasta_collect
					int idFastaInfo = this.dao.getIDFastaInfo(file.getName());
					
					this.lineNumber = 0;
					
					this.readFastaFile(file.getAbsolutePath(), idFastaInfo);
					System.out.println("** Fim da leitura do arquivo: "+file.getName());
				}else {
					System.out.println("*** Erro "+file.getName()+ " não é um arquivo .fasta");
				}
			}
		}
	}
	/**
	 * Ler um Fasta especifico e insere no Cassandra
	 * @param fastaFile
	 */
	public void readFastaFile(String fastaFile, int idFastaInfo){
		BufferedReader br = null;
		String line = "";
		String fastaSplitBy = "\n";
	 
		int numOfLine = 0;
		try {
			br = new BufferedReader(new FileReader(fastaFile));
			String idSeq = "";
			String seqDNA = "";
			System.out.println("**** Processando o arquivo fasta");
			List<String> allQuery = new ArrayList<String>();
			while ((line = br.readLine()) != null) {
				numOfLine++;
				this.allLines++;
				this.lineNumber++;
				String[] brokenFasta = line.split(fastaSplitBy);
				if (numOfLine%2 == 1){
					idSeq = brokenFasta[0];
				}else if (numOfLine > 1){
					seqDNA = brokenFasta[0];
				}
				if (numOfLine%2 == 0){
					String query = "INSERT INTO fasta_collect (id_seq, seq_dna, line, fasta_info) VALUES ('"+idSeq+"', '"+seqDNA+"',"
							+ " "+this.lineNumber/2+", "+idFastaInfo+");";
					allQuery.add(query);
					idSeq = "";
					seqDNA = "";
				}
			}
			System.out.println("**** Inserindo no MySQL...");
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
					System.out.println("Quantidade de registros inseridos: "+i);
				}
			}
			this.dao.afterExecuteQuery();
			System.out.println("**** Total de linhas inseridas no Banco: "+this.allLines/2);
		}catch (Exception e){
			System.out.println("Erro ao executar a query :( \n"+e.getMessage());
		}
	}

}
