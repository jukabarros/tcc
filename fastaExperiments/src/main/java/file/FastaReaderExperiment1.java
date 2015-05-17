package file;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import dao.Experiment1DAO;

public class FastaReaderExperiment1 {
	
	public int lines;
	
	private Experiment1DAO dao;

	public FastaReaderExperiment1() {
		super();
		this.lines = 0;
		this.dao = new Experiment1DAO();
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * Ler um CSV especifico e insere no Cassandra
	 * @param csvFile
	 */
	public void readFastaFile(String fastaFile){
		BufferedReader br = null;
		String line = "";
		String fastaSplitBy = "\n";
	 
		int numOfLine = 0;
		try {
			br = new BufferedReader(new FileReader(fastaFile));
			String id = null;
			String seqDNA = null;
			while ((line = br.readLine()) != null) {
				numOfLine++;
				this.lines++;
				String[] brokenFasta = line.split(fastaSplitBy);
				if (numOfLine%2 == 1){
					id = brokenFasta[0];
				}else if (numOfLine > 1){
					seqDNA = brokenFasta[0];
				}
				if (numOfLine%2 == 0){
					String allData = id+":"+seqDNA;
					dao.insert(allData);
					id = null;
					seqDNA = null;
				}
				if (this.lines%1000 == 0){
					System.out.println("Numero de registros inseridos: "+this.lines/2);
				}
				
			}
			System.out.println("**** NUM OF LINE TOTAL FASTA FILE: "+this.lines);
			System.out.println("TOTAL: "+this.lines/2);
	 
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
