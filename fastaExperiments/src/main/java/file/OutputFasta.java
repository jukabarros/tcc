package file;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import config.ReadProperties;
import dna.FastaContent;

/*
 * Classe responsavel por criar o arquivo fasta apos 
 * a consulta do BD
 */
public class OutputFasta {
	
	private FileWriter fw;
	private File file;
	
	// Lista do conteudo que sera escrito
	private List<FastaContent> allFastaContent;
	// Lista que ordena o conteudo vindo do bd
	private List<Integer> order;
	
	private Properties prop;
	
	public OutputFasta() throws IOException {
		this.fw = null;
		this.file = null;
		this.allFastaContent = new ArrayList<FastaContent>();
		this.order = new ArrayList<Integer>();
		this.prop = ReadProperties.getProp();
	}
	
	public void createFastaFile(String filename) throws IOException{
		this.file = new File(filename);
		 
		if (!file.exists()) {
			file.createNewFile();
		}
		this.fw = new FileWriter(this.file.getAbsoluteFile(), true);
	}
	
	
	/**
	 * Ordena as linhas do arquivo fasta, de modo que, fique com a mesma
	 * sequencia do arquivo original
	 * 
	 * Cria um MAP que tera a chave a linha e o valor id:seqDNA e que em seguida
	 * feito um tratamento para criar o arquivo
	 * 
	 * Em seguida escreve o arquivo de saida
	 * 
	 * @param listFastaInfo 
	 */
	public void prepareFastaFile(List<FastaContent> listFastaContent){
		
		Map<Integer, String> orderSequence = new HashMap<Integer, String>();
		for (int i = 0; i < this.allFastaContent.size(); i++) {
			int position = listFastaContent.get(i).getLine();
			String content = listFastaContent.get(i).getId()+":"+listFastaContent.get(i).getSeqDNA();
			orderSequence.put(position, content);
			this.order.add(position);
		}
		Collections.sort(this.order);
		
		for (int i = 0; i < this.order.size(); i++) {
			String value = orderSequence.get(this.order.get(i));
			String[] brokenValue = value.split(":");
			String	id = brokenValue[0];
			String	seqDNA = brokenValue[1];
			this.writeFastaFile(id, seqDNA);
		}
	}
	
	
	/**
	 * Cria o arquivo fasta quando a qntd de SRSs por linha eh igual a 1
	 * essa qntd eh definida no arquivo properties: srs.quantity
	 * @param id
	 * @param seqDNA
	 */
	public void writeFastaFile(String id, String seqDNA){
		try {
			int srsSize = Integer.parseInt(prop.getProperty("srs.quantity"));
			if (srsSize > 1){
				
				String[] brokenStr = id.split(">");
				int breakSeq = 0;
				int seqDNAlength = seqDNA.length()/srsSize;

				for (int i = 1; i <= srsSize; i++) {
					CharSequence uniqueSequenceDNA = seqDNA.subSequence(breakSeq, breakSeq+seqDNAlength);
					this.fw.write(">"+brokenStr[i]+'\n'+uniqueSequenceDNA+'\n');
					breakSeq += seqDNAlength;
				}
				
			}else{
				this.fw.write(id+'\n'+seqDNA+'\n');
			}
		} catch (IOException ex) {
			System.out.println("Erro na criação do arquivo fasta: "+ex.getMessage());
		} 

	}
	
	public void closeFastaFile() throws IOException{
		this.fw.close();
	}

}
