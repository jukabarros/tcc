package file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import dao.Experiment1DAO;

public class FastaReader {
	
	public int lines;
	
	private Experiment1DAO dao;

	public FastaReader() {
		super();
		this.lines = 0;
		this.dao = new Experiment1DAO();
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * Ler um CSV especifico e insere no Cassandra
	 * @param csvFile
	 */
	public void readFastaFile(File fastaFile){
		String fastaFileExample = "/home/juccelino/Desktop/bioData/orig/test29";
		BufferedReader br = null;
		String line = "";
		String fastaSplitBy = "\n";
	 
		int numOfLine = 0;
		try {
			br = new BufferedReader(new FileReader(fastaFileExample));
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
//					System.out.println(id +" : "+ seqDNA + " >> LINE: "+this.lines/2);
					id = null;
					seqDNA = null;
					String allData = id+":"+seqDNA;
					dao.insert(allData);
//					break;
				}
				if (this.lines%1000 == 0){
					System.out.println("Numero de registros inseridos: "+this.lines);
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
		this.lines += numOfLine; 
	  }
}
