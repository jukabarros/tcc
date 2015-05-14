package config;

import java.sql.Connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class ConnectCassandra {
	
	private Connection conn;
	private Cluster cluster;
	
	public Connection connect(){
		try{
			this.cluster = Cluster.builder()
			  .addContactPoint("150.161.11.134")
			  .build();
			
		}catch (Exception e){
			System.out.println("Erro ao se conectar com o BD: "+e.getMessage());
		}
		return conn;
	}
	
	public void getConnectionInfo(){
		this.connect();
		Metadata metadata = this.cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", 
				metadata.getClusterName());
		for ( Host host : metadata.getAllHosts() ) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		this.close();
	}
	
	public void close() {
		   this.cluster.close();
		}
	
	// GET AND SET
	public Cluster getCluster() {
		return cluster;
	}

}
