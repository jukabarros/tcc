package dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import create.MongoDBCreate;
import dna.FastaContent;

public class MongoDBDAO {
	
	private MongoDBCreate mongoDBCreate;
	private DBCollection dbCollection;
	
	public MongoDBDAO() throws IOException {
		super();
		this.mongoDBCreate = new MongoDBCreate();
	}
	
	/*
	 * Pega a collection, se nao existir
	 * cria uma nova
	 */
	public void getCollection(String collection) throws IOException{
		System.out.println("** Collection "+collection);
		this.dbCollection = this.mongoDBCreate.getCollection(collection);
	}
	
	public void insertFastaInfo(String fileName, String comment, long size) throws IOException{
		/**** Insert ****/
		this.dbCollection = this.mongoDBCreate.getCollection("fasta_info");
		BasicDBObject document = new BasicDBObject();
		document.put("file_name", fileName);
		document.put("size", size);
		document.put("comment", comment);
		this.dbCollection.insert(document);
		
		this.dbCollection = null;
	}
	
	public void insertData(String id, String seqDna, int line) throws IOException{
		/**** Insert ****/
		// create a document to store key and value
		BasicDBObject document = new BasicDBObject();
		document.put("id", id);
		document.put("seqDna", seqDna);
		document.put("line", line);
		this.dbCollection.insert(document);
	}
	
	/**
	 * Retorna o conteudo de uma collection, ou seja,
	 * de um arquivo fasta completo e manda para a lista de Fasta_Info
	 * onde pode ser gerado o arquivo fasta
	 * @param collection
	 * @throws IOException
	 */
	public List<FastaContent> findByCollection(String collection) throws IOException{
		/**** Find and display ****/
		this.dbCollection = this.mongoDBCreate.getCollection(collection);
		DBCursor cursor = this.dbCollection.find();
		List<FastaContent> listFastaContent = new ArrayList<FastaContent>();
		while (cursor.hasNext()) {
			// Daqui mandar para a lista de Fasta_Info
//			FastaContent fastaContent = new FastaContent(id, seqDNA, line)
			System.out.println(cursor.next());
			break;
		}
		if (listFastaContent.isEmpty()){
			System.out.println("*** Conteúdo do arquivo não encontrado no Banco de dados :(");
		}
		return listFastaContent;
	}
	
	/**
	 * Procura por um id de sequencia especifico
	 * em todas as collections
	 * @param id
	 * @throws IOException
	 */
	public void findByID(String id) throws IOException{
		/**** Find and display ****/
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("id", id);

		DBCursor cursor = this.dbCollection.find(searchQuery);

		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	
	
	public void findAll() throws IOException{

		DBCursor cursor = this.dbCollection.find();

		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}

}
