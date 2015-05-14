package com.juka;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import csv.CSVReader;
import dao.CassandraDAO;

public class Application {

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		String dir = "/home/juccelino/Desktop/csvs/specdata";
		CSVReader csv = new CSVReader();
		csv.readAllCSV(dir);
		CassandraDAO dao = new CassandraDAO();
//		dao.selectAll();
		
		long endTime = System.currentTimeMillis();
		System.out.println("\n******** TOTAL LINE: "+csv.lines);

		long totalTime = endTime - startTime;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("******** EXECUTION TIME: " + formatter.format(totalTime / 1000d) + " seconds");
	}

}
