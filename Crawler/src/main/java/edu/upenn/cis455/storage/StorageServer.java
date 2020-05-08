package edu.upenn.cis455.storage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import edu.upenn.cis455.crawler.XPathCrawler;

public class StorageServer {

	private DBWrapper myDB;
	private AWSCredentials credentials;
	private AmazonS3 s3client;
	private static final StorageServer instance = new StorageServer();
	private final String BUCKET_NAME = "cis455-group13-crawldata";
	
	private StorageServer() { 
		
	}
	
	public static StorageServer getInstance() {
		return instance;
	}
	
	public  void init(String directory) {
		try {
			getInstance().myDB = new DBWrapper(directory);
		//	
		//	if(s3client.doesBucketExist(BUCKET_NAME)) {
			//	System.out.println("s3 Bucket Name already exists");
			//	return;
		//	}
			//s3client.createBucket(BUCKET_NAME); Bucket already created
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
	
	/*Add user information to the database using the username as the key  /
	public void addUserInfo(String username, UserVal val) {
		
		//declare database entry key-value pair
		DatabaseEntry value = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry(username.getBytes());
		
		myDB.getUserValBinding().objectToEntry(val, value);
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			myDB.getUserDB().put(txn, key, value);
			
			txn.commit(); //commit transaction
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}				
	} */
	
	/* Get user information from database using the username as the key  /
	public UserVal getUserInfo(String username) {
		DatabaseEntry value = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry(username.getBytes());
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			if(myDB.getUserDB().get(txn, key, value, LockMode.DEFAULT) != OperationStatus.SUCCESS){
				return null;
			}
					
			txn.commit(); //commit transaction
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}	
		return (UserVal) myDB.getUserValBinding().entryToObject(value);
	} */
	
	/*Add crawled document to database. URL is the key  */
	public void addDocInfo(String url, DocVal val) {
			
		//declare database entry key-value pair
		DatabaseEntry value = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry(url.getBytes());
		
		myDB.getDocValBinding().objectToEntry(val, value);
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			myDB.getDocDB().put(txn, key, value);
			
			txn.commit(); //commit transaction
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}				
	}
	
	/*Retrieve crawled document from the database based on URL key */
	public DocVal getDocInfo(String url) {
		DatabaseEntry value = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry(url.getBytes());
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			if(myDB.getDocDB().get(txn, key, value, LockMode.DEFAULT) != OperationStatus.SUCCESS){
				return null;
			}
					
			txn.commit(); //commit transaction
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}	
		return (DocVal) myDB.getDocValBinding().entryToObject(value);
	}
	
	/*Retrieve crawled document from the database based on URL key */
	public int addURL(String url) {
		DatabaseEntry value = new DatabaseEntry("".getBytes()); //empty
		DatabaseEntry key = new DatabaseEntry(url.getBytes());
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			 if (myDB.getSeenDB().get(txn, key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			//	System.out.println("seen: " + url);
				 txn.commit();
				return 1; //key exists
			}else {
				myDB.getSeenDB().put(txn, key, value);
				txn.commit();
				return 0;
			}					
		} catch (Exception e) {
			txn.abort();
			System.out.println("error: " );
			e.printStackTrace();
			return -1;
		}	
	}
	
	/*Retrieve crawled document from the database based on URL key */
	public int addContentHash(String hash) {
		DatabaseEntry value = new DatabaseEntry("".getBytes()); //empty
		DatabaseEntry key = new DatabaseEntry(hash.getBytes());
		
		//begin transaction
	//	Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			 if (myDB.getContentDB().get(null, key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			//	System.out.println("seen: " + url);
				return 1; //key exists
			}else {
				myDB.getContentDB().put(null, key, value);
				return 0;
			}					
		} catch (Exception e) {
			System.out.println("error: " );
			e.printStackTrace();
			return -1;
		}	
	}
	
	/*Add a new channel to database. Channel Name is the key  */
	public void addChannelInfo(String name, ChannelStorage val) {
			
		//declare database entry key-value pair
		DatabaseEntry value = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry(name.getBytes());
		
		myDB.getChannelValBinding().objectToEntry(val, value);
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			myDB.getChannelDB().put(txn, key, value);
			
			txn.commit(); //commit transaction
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}				
	}
	
	public int deleteChannel(String name) {	
		DatabaseEntry key = new DatabaseEntry(name.getBytes());
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {			
			if(myDB.getChannelDB().delete(txn, key) != OperationStatus.SUCCESS) {
				return 0;
			} else {
				txn.commit(); //commit transaction
				return 1;
			}											
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}	
		return 0;
	}
	
	/*Retrieve Channel info from the database based on Channel Name key, null if specified channel doesn't exist */
	public ChannelStorage getChannelInfo(String name) {
		DatabaseEntry value = new DatabaseEntry();
		DatabaseEntry key = new DatabaseEntry(name.getBytes());
		
		//begin transaction
		Transaction txn = myDB.getEnv().beginTransaction(null, null);
		try {
			if(myDB.getChannelDB().get(txn, key, value, LockMode.DEFAULT) != OperationStatus.SUCCESS){
				return null;
			}
					
			txn.commit(); //commit transaction
		} catch (Exception e) {
			if(txn != null) {
				txn.abort();
				txn = null;
			}
		}	
		return (ChannelStorage) myDB.getChannelValBinding().entryToObject(value);
	}
	
	public boolean exists(String url) { //check if url already exists in DB
		try {
		    // Create a pair of DatabaseEntry objects. theKey
		    // is used to perform the search. theData is used
		    // to store the data returned by the get() operation.
		    DatabaseEntry theKey = new DatabaseEntry(url.getBytes("UTF-8"));
		    DatabaseEntry value = new DatabaseEntry();
		    
		    if (myDB.getSeenDB().get(null, theKey, value, LockMode.DEFAULT) ==
		        OperationStatus.SUCCESS) {
		    	return true;
		        
		    } else {
		        return false;
		    } 
		} catch (Exception e) {
		    e.printStackTrace();
		}
		return true;
	}
	
	public HashMap<String, ChannelStorage> getAllChannels() {
		HashMap<String, ChannelStorage> channels = new HashMap<String, ChannelStorage>();
		Cursor cursor = null;
		try {
		   
		    // Open the cursor. 
		    cursor = myDB.getChannelDB().openCursor(null, null);

		    // Get the DatabaseEntry objects that the cursor will use.
		    DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();

		    // Iterate from the last record to the first in the database
		    while (cursor.getPrev(foundKey, foundData, LockMode.DEFAULT) == 
		        OperationStatus.SUCCESS) {

		        String theKey = new String(foundKey.getData());
		        channels.put(theKey, (ChannelStorage) myDB.getChannelValBinding().entryToObject(foundData));
		    }
		} catch (DatabaseException de) {
		    System.err.println("Error accessing database." + de);
		} finally {
		    // Cursors must be closed.
		    cursor.close();
		}		
		return channels;
	}
	
	public void writetoFile(String directory) {
		/*initialize s3 client /
		getInstance().credentials = new BasicSessionCredentials(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);
			getInstance().s3client = AmazonS3ClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials))
					.withRegion(Regions.US_EAST_1)
					.build(); */
		BufferedWriter writer;
		int count = 0;
		try {
			writer = new BufferedWriter(new FileWriter(directory));
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}
		Cursor cursor = null;
		try {
		   
		    // Open the cursor. 
		    cursor = myDB.getDocDB().openCursor(null, null);

		    // Get the DatabaseEntry objects that the cursor will use.
		    DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();

		    // Iterate from the last record to the first in the database
		    while (cursor.getPrev(foundKey, foundData, LockMode.DEFAULT) == 
		        OperationStatus.SUCCESS) {

		        String url = new String(foundKey.getData());
		        String body = ((DocVal) myDB.getDocValBinding().entryToObject(foundData)).getBody();
		        body = body.replaceAll("\n", "");
		        body = body.replaceAll("\r", ""); //remove CLRF so that each webpage will be on a single line
		        writer.write(url + "$$$" + body  + "\n");
		        writer.flush();
		        count++;
		    }
		} catch (DatabaseException de) {
		    System.err.println("Error accessing database." + de);
		} catch (IOException e) {
			System.out.println("error writing to file");
			e.printStackTrace();
		} finally {
		    // Cursors must be closed.
		    cursor.close();
		    try {
				//writer.flush();
				writer.close();				
			} catch (IOException e) {				
				e.printStackTrace();
			}
		  //  getInstance().s3client.putObject(BUCKET_NAME, "CorpusA/corpusA.txt", new File(directory)); //TODO: replace crawA with node name
		    
		}
		//int written  = XPathCrawler.getInstance().getFileCount().get();
		//System.out.println("Crawler downloaded: " + written + " files");
		System.out.println("total num_files: " + count);
		return;
	}
	
	public void writetoS3(String directory) {
		/* initialize s3 client */
		JSONParser parser = new JSONParser();
		try {
			String file = System.getProperty("user.dir")+"/CrawlerConfigs/s3_config.json";
            Object obj = parser.parse(new FileReader(file));

            JSONObject jsonObj = (JSONObject) obj;
            
            JSONArray access_key = (JSONArray) jsonObj.get("accKey");
            JSONArray secret_key = (JSONArray) jsonObj.get("secKey");
            JSONArray session_token = (JSONArray) jsonObj.get("seshToken");
                                            
            String accessKey = access_key.toArray()[0].toString();
            String secretKey = secret_key.toArray()[0].toString();
            String sessionToken = session_token.toArray()[0].toString();
            
            getInstance().credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
			getInstance().s3client = AmazonS3ClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials))
					.withRegion(Regions.US_EAST_1)
					.build(); 
		
			getInstance().s3client.putObject(BUCKET_NAME, "CorpusD_0", new File(directory)); // replace crawA with node name
		} catch(AmazonS3Exception | IOException | ParseException e) {
			e.printStackTrace();
			return;
		}					    			
			return;
	}
	
	public int getFileCount() {
				
		Cursor cursor = null;
		int count = 0;
		try {		   
		    // Open the cursor. 
		    cursor = myDB.getDocDB().openCursor(null, null);

		    // Get the DatabaseEntry objects that the cursor will use.
		    DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();

		    // Iterate from the last record to the first in the database
		    while (cursor.getPrev(foundKey, foundData, LockMode.DEFAULT) == 
		        OperationStatus.SUCCESS) {
		        
		        count++;
		    }
		} catch (DatabaseException de) {
		    System.err.println("Error accessing database." + de);
		}  finally {
		    // Cursors must be closed.
		    cursor.close();		    		    
		}
		return count;
	}
	
	public int getLinksCrawledCount() {
		
		Cursor cursor = null;
		int count = 0;
		try {		   
		    // Open the cursor. 
		    cursor = myDB.getSeenDB().openCursor(null, null);

		    // Get the DatabaseEntry objects that the cursor will use.
		    DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();

		    // Iterate from the last record to the first in the database
		    while (cursor.getPrev(foundKey, foundData, LockMode.DEFAULT) == 
		        OperationStatus.SUCCESS) {
		        
		        count++;
		    }
		} catch (DatabaseException de) {
		    System.err.println("Error accessing database." + de);
		}  finally {
		    // Cursors must be closed.
		    cursor.close();		    		    
		}
		return count;
	}
	
	/*
	public HashMap<String, UserVal> getAllUsers() {
		HashMap<String, UserVal> users = new HashMap<String, UserVal>();
		Cursor cursor = null;
		try {		   
		    // Open the cursor. 
		    cursor = myDB.getUserDB().openCursor(null, null);

		    // Get the DatabaseEntry objects that the cursor will use.
		    DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();

		    // Iterate from the last record to the first in the database
		    while (cursor.getPrev(foundKey, foundData, LockMode.DEFAULT) == 
		        OperationStatus.SUCCESS) {

		        String theKey = new String(foundKey.getData());
		        users.put(theKey, (UserVal) myDB.getUserValBinding().entryToObject(foundData));
		    }
		} catch (DatabaseException de) {
		    System.err.println("Error accessing database." + de);
		} finally {
		    // Cursors must be closed.
		    cursor.close();
		}		
		return users;
	} */
	
	public void close() {
		getInstance().myDB.closeDB();
	}
}
