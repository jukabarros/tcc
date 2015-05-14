package main;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import dao.Experiment1DAO;
import file.FastaReader;

public class Application {

	public static void main(String[] args) {
		FastaReader fr = new FastaReader();
		long startTime = System.currentTimeMillis();
//		fr.readFastaFile(null);
		Experiment1DAO dao = new Experiment1DAO();
		dao.selectAll();
		long endTime = System.currentTimeMillis();
		
		long totalTime = endTime - startTime;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n******** EXECUTION TIME: " + formatter.format(totalTime / 1000d) + " seconds");
		System.out.println("**** NUM OF LINE TOTAL: "+fr.lines);


	}

}
