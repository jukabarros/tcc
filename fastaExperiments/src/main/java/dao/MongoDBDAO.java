package dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
	
	public void insertData(String idSeq, String seqDna, int line) throws IOException{
		/**** Insert ****/
		// create a document to store key and value
		BasicDBObject document = new BasicDBObject();
		document.put("idSeq", idSeq);
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
		this.dbCollection = this.mongoDBCreate.getCollection(collection);
		DBCursor cursor = this.dbCollection.find();
		List<FastaContent> listFastaContent = new ArrayList<FastaContent>();
		while (cursor.hasNext()) {
		    BasicDBObject obj = (BasicDBObject) cursor.next();
			FastaContent fastaContent = new FastaContent(obj.getString("idSeq"), obj.getString("seqDna"), obj.getInt("line"));
			listFastaContent.add(fastaContent);
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
	public void findByID(String idSeq) throws IOException{
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("idSeq", idSeq);
		Set<String> allCollections = this.mongoDBCreate.listAllColection();
		String[] listAllCollections = allCollections.toArray(new String [allCollections.size()]);
		List<FastaContent> listFastaContent = new ArrayList<FastaContent>();
		for (int i = 0; i < listAllCollections.length; i++) {
			this.dbCollection = this.mongoDBCreate.getCollection(listAllCollections[i]);
			DBCursor cursor = this.dbCollection.find(searchQuery);
			while (cursor.hasNext()) {
				System.out.println("** ID encontrado na coleção "+listAllCollections[i]);
				BasicDBObject obj = (BasicDBObject) cursor.next();
				System.out.println("ID: "+obj.getString("idSeq"));
				System.out.println("Sequência: "+obj.getString("seqDna"));
				System.out.println("Linha: "+obj.getInt("line"));
				FastaContent fastaContent = new FastaContent(obj.getString("idSeq"), obj.getString("seqDna"), obj.getInt("line"));
				listFastaContent.add(fastaContent);
			}
		}
		if (listFastaContent.isEmpty()){
			System.out.println("*** ID de Sequência não encontrado no Banco de dados :(");
		}
	}
	
	
	public void findAll() throws IOException{

		DBCursor cursor = this.dbCollection.find();

		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	
}
