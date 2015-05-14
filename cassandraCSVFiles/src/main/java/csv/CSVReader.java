package csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import dao.CassandraDAO;



public class CSVReader {
	
	public int lines = 0;

	CassandraDAO dao = new CassandraDAO();
	/**
	 * Ler todos os CSV de um repositorio especifico
	 * @param csvDirectory
	 */
	public void readAllCSV(String csvDirectory){
		File directory = new File(csvDirectory);

		//get all the files from a directory
		File[] fList = directory.listFiles();
		for (File file : fList){
			if (file.isFile()){
				System.out.println("Lendo o arquivo: "+file.getName());
				this.readCSVFile(file);
				System.out.println("OK");
			}
		}
	}
	
	/**
	 * Ler um CSV especifico e insere no Cassandra
	 * @param csvFile
	 */
	public void readCSVFile(File csvFile){
		String csvFileExample = "/home/juccelino/Desktop/csvs/specdata/001.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
	 
		int numOfLine = 0;
		try {
			/*
			 * TO DO
			 * Tirar a primeira linha de alguns csvs que possui
			 * o cabecalho
			 */
			br = new BufferedReader(new FileReader(csvFileExample));
			int headerLine = 0;
			while ((line = br.readLine()) != null) {
				numOfLine++;
				if (headerLine == 0){
					headerLine++;
					continue;
				}
			    // use comma as separator
				String[] brokenCsv = line.split(cvsSplitBy);
				String dateStr = brokenCsv[0];
				String dateBroken = dateStr.substring(1, dateStr.length() -1);
				String sulfate = brokenCsv[1];
				String nitrate = brokenCsv[2];
				String id = brokenCsv[3];
				Date dateOfColect = this.strToDate(dateBroken);
				dao.insert(dateOfColect, sulfate, nitrate, Integer.parseInt(id));
//				System.out.println(date +" - "+ sulfate +" - "+ nitrate +" - "+ id);
	 
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
//		System.out.println("*************************************");
//		System.out.println("Number of Line: "+numOfLine);
//		System.out.println("Done");
		this.lines += numOfLine; 
	  }
	
	private Date strToDate(String dateStr){
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-mm-dd");
		try{
			Date date = formatter.parse(dateStr);
			return date;
		}catch (ParseException e){
			Date dateErr = new Date();
			System.err.println("**** Nao foi possivel converter "+dateStr+ 
					"para data. \nErro: "+e.getMessage());
			return dateErr;
		}
		
	}

}
