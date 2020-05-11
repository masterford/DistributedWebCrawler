/*
 * @author:Ransford Antwi
 */

package edu.upenn.cis455.storage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

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
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


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
		
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
	
	
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
		txn.setLockTimeout(2, TimeUnit.SECONDS);
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
		} catch(LockConflictException e) {
			txn.commit();
			return -1;
			
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
	
	
	/*Writes content in DB to file. Each file will have at 
	 * most 10000 lines where each line is a webpage  */
	public void writetoFile(String directory) {
		
		BufferedWriter writer;
		int num_files = 0; 
		int count = 0;
		int fileCount = 0;
		try {
			writer = new BufferedWriter(new FileWriter(directory + "_" + num_files + ".txt"));
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
		    String url;
		    String body;
		    while (cursor.getPrev(foundKey, foundData, LockMode.DEFAULT) == 
		        OperationStatus.SUCCESS) {
		    	if(fileCount == 10000) { //new file
		    		System.out.println("writing new file");
		    		num_files++;
		    		writer.close();
		    		writer = new BufferedWriter(new FileWriter(directory + "_" + num_files + ".txt"));
		    		fileCount = 0; //reset line counter
		    	}
		        url = new String(foundKey.getData());
		        body = ((DocVal) myDB.getDocValBinding().entryToObject(foundData)).getBody();
		        body = body.replaceAll("\n", "");
		        body = body.replaceAll("\r", ""); //remove CLRF so that each webpage will be on a single line
		        writer.write(url + "$$$" + body  + "\n");
		        writer.flush();
		        count++;
		        fileCount++;
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
		System.out.println("total pages: " + count);
		System.out.println("num files: " + num_files);
		return;
	}
	
	/*Upload content to Amazon S3  */
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
	
	public void close() {
		getInstance().myDB.closeDB();
	}
}
