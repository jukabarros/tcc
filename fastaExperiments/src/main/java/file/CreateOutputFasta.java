package file;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/*
 * Classe responsavel por criar o arquivo fasta apos 
 * a consulta do BD
 */
public class CreateOutputFasta {
	
	/*
	 * Cria o arquivo fasta quando a qntd de SRRs por linha eh igual a 1
	 * essa qntd eh definida no arquivo properties: srr.quantity
	 */
	public void createSingleFastaFile(String id, String seqDNA){
		Writer writer = null;
		
		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("filename.txt"), "utf-8"));
			writer.write("Something");
		} catch (IOException ex) {
			// report
		} finally {
			try {writer.close();} catch (Exception ex) {/*ignore*/}
		}

	}
}
