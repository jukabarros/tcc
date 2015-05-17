package main;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import dao.Experiment1DAO;
import dao.Experiment2DAO;
import file.FastaReaderExperiment1;

public class Application {

	public static void main(String[] args) {
//		String fastaFile = "/home/juccelino/Desktop/bioData/orig/test29";
		FastaReaderExperiment1 fr = new FastaReaderExperiment1();
		long startTime = System.currentTimeMillis();
//		fr.readFastaFile(fastaFile);
		Experiment2DAO dao = new Experiment2DAO();
		dao.selectAll();
		long endTime = System.currentTimeMillis();
		
		long totalTime = endTime - startTime;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n******** EXECUTION TIME: " + formatter.format(totalTime / 1000d) + " seconds");
	}

}
