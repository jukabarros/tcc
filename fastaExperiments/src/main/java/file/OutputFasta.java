package file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dna.FastaInfo;

/*
 * Classe responsavel por criar o arquivo fasta apos 
 * a consulta do BD
 */
public class OutputFasta {
	
	private FileWriter fw;
	private BufferedWriter bw;
	
	public OutputFasta() throws IOException {
		this.fw = null;
		this.bw = null;
	}
	
	public void createFastaFile(String filename) throws IOException{
		File file = new File(filename);
		 
		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}
		this.fw = new FileWriter(file.getAbsoluteFile());
		this.bw = new BufferedWriter(fw);
	}
	
	
	/**
	 * Ordena as linhas do arquivo fasta, de modo que, fique com a mesma
	 * sequencia do arquivo original
	 * 
	 * Cria um MAP que tera a chave a linha e o valor id:seqDNA e que em seguida
	 * feito um tratamento para criar o arquivo
	 * 
	 * @param listFastaInfo 
	 */
	public void prepareFastaFile(List<FastaInfo> listFastaInfo){
		List<Integer> order = new ArrayList<Integer>();
		Map<Integer, String> orderSequence = new HashMap<Integer, String>();
		
		for (int i = 0; i < listFastaInfo.size(); i++) {
			int position = listFastaInfo.get(i).getLine();
			String content = listFastaInfo.get(i).getId()+":"+listFastaInfo.get(i).getSeqDNA();
			orderSequence.put(position, content);
			order.add(position);
		}

		Collections.sort(order);
		
		for (int i = 0; i < order.size(); i++) {
			String value = orderSequence.get(order.get(i));
			String[] brokenValue = value.split(":");
			String	id = brokenValue[0];
			String	seqDNA = brokenValue[1];
			this.writeFastaFile(id, seqDNA);
		}
	}
	
	
	/**
	 * Cria o arquivo fasta quando a qntd de SRRs por linha eh igual a 1
	 * essa qntd eh definida no arquivo properties: srr.quantity
	 * @param id
	 * @param seqDNA
	 */
	public void writeFastaFile(String id, String seqDNA){
		try {
			this.bw.write(id+'\n'+seqDNA+'\n');
		} catch (IOException ex) {
			System.out.println("Erro na criação do arquivo fasta: "+ex.getMessage());
		} 

	}
	
	public void closeFastaFile() throws IOException{
		this.bw.close();
	}
}
