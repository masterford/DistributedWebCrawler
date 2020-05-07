package edu.upenn.cis455.storage;

import java.io.File;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
//import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;

/** (MS1, MS2) A wrapper class which should include:
  * - Set up of Berkeley DB
  * - Saving and retrieving objects including crawled docs and user information
  */
public class DBWrapper {
	
	private static String envDirectory = null;
	
	//Instantiate string indentities
	private static final String SEEN_STORE = "seen_store"; //name used to identify seenURLDB
	private static final String DOC_STORE = "doc_store"; //name used to identify docDB
	private static final String CONTENT_STORE = "content_store"; //name used to identify content hash store
	private static final String CHANNEL_STORE = "channel_store"; //name used to identify channelDB
	private static final String CLASS_CATALOG = "class_catalog"; //name used to identify the class catalog
	
	//Declare databases
	private StoredClassCatalog catalog =null;
	private Database seenURLDB = null;
	private Database contentDB = null; //db for content hashes
	private Database catalogDB = null;
	private Database docDB = null;
	private Database channelDB = null;
	
//	private StoredSortedMap userMap = null;
	
	//Instantiate Bindings for key and value classes
	//EntryBinding<UserVal> userValBinding;
	EntryBinding<DocVal> docValBinding;
	EntryBinding<ChannelStorage> channelValBinding;
	
	private static Environment myEnv;
	private static EntityStore store;
	
	public DBWrapper(String directory) {
		
		try {
		//Instantiate Environments
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(true); //allow transactions
		envConfig.setAllowCreate(true); //create if doesn't exist already
		DBWrapper.myEnv = new Environment(new File(directory), envConfig); //instantiate environment		
		
		//DB instantiation
		DatabaseConfig dbConfig = new DatabaseConfig();
		DatabaseConfig dbTempConfig = new DatabaseConfig();
		dbConfig.setTransactional(true); //encloses the database open within a transaction
		dbConfig.setAllowCreate(true); //create DB if it doesn't exist already
		dbTempConfig.setAllowCreate(true); //create DB if it doesn't exist already
		dbTempConfig.setTemporary(true);
		
		this.catalogDB = myEnv.openDatabase(null, CLASS_CATALOG, dbConfig);
		this.catalog = new StoredClassCatalog(this.catalogDB);		
		this.docDB = myEnv.openDatabase(null, DOC_STORE, dbConfig);
		this.channelDB = myEnv.openDatabase(null, CHANNEL_STORE, dbConfig);
		this.contentDB = myEnv.openDatabase(null, CONTENT_STORE, dbTempConfig);
		this.seenURLDB = myEnv.openDatabase(null, SEEN_STORE, dbConfig);
		
		//this.userValBinding = new SerialBinding<UserVal>(this.catalog, UserVal.class);
		this.docValBinding = new SerialBinding<DocVal>(this.catalog, DocVal.class);
		this.channelValBinding = new SerialBinding<ChannelStorage>(this.catalog, ChannelStorage.class);
		} catch(DatabaseException e) {
			e.printStackTrace();
		}		
	}
	
	public Environment getEnv() {
		return DBWrapper.myEnv;
	}
	
	public Database getSeenDB() {
		return this.seenURLDB;
	}
	
	public Database getDocDB() {
		return this.docDB;
	}
	
	public Database getChannelDB() {
		return this.channelDB;
	}
	
	public Database getContentDB() {
		return this.contentDB;
	}
	
	public void closeDB() {
		this.seenURLDB.close();
		this.docDB.close();		
		this.catalogDB.close();
		this.channelDB.close();
		this.contentDB.close();
		myEnv.close();
	}
	//public EntryBinding<UserVal> getUserValBinding() {
	//	return this.userValBinding;
	//}
	
	public EntryBinding<DocVal> getDocValBinding() {
		return this.docValBinding;
	}
	
	public EntryBinding<ChannelStorage> getChannelValBinding() {
		return this.channelValBinding;
	}
	
}
