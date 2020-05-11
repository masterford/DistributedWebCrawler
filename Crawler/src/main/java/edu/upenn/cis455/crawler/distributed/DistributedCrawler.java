/* *
 * @author: Ransford Antwi
 */

package edu.upenn.cis455.crawler.distributed;

import edu.upenn.cis455.storage.StorageServer;
import edu.upenn.cis455.crawler.URLFrontier;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.crawler.info.*;

/** (MS1, MS2) The main class of the crawler.
  */
public class DistributedCrawler {
  private int maxDocSize;
  private int maxFileNum;
  private String hostNameMonitoring;
  private HashMap<String, RobotsTxtInfo> robotMap; //caches the results of robots.txt for each domain
  public static HashSet<String> bannedHosts; //set to maintain banned hosts, normalized
  public static HashSet<String> whiteList; //set to maintain hosts that incorrectly get identified as non english
 // private HashSet<String> seenContent; //set to maintain seen content
  private HashMap<String, Date> lastCrawled; //data structure to keep track of the last time a hostname server was crawled
  private InetAddress hostMonitor;
  private AtomicInteger fileCount;
  private AtomicInteger linksCrawled;
  private AtomicInteger http2xx; //number of http 200 requests accepted
  private AtomicInteger http404; //number of not found reponses
  private AtomicInteger http3xx; //number of redirects
  private AtomicInteger httpOther; //other response types
  private AtomicInteger inFlightMessages; //used to keep track of messages being routed so that we don't shutdown prematurely
  private URLFrontier frontier;
  private File diskFile;
  public static final int FRONTIER_BUFFER_SIZE = 1000; //number of URLS to keep in memory
  public static String storagePath;
  private boolean shutdownCalled = false; //flag to keep track of whether the shutdown method has already been called
  private volatile boolean shutdown = false; //flag to keep track of whether to shutdown or not
  
  private static final String URL_SPOUT = "URL_SPOUT";
  private static final String CRAWLER_BOLT = "CRAWLER_BOLT";
  private static final String DOCPARSER_BOLT = "DOCPARSER_BOLT";
  private static final String HOST_BOLT = "HOST_BOLT";
  private static final String URLFILTER_BOLT = "URLFILTER_BOLT";
  private final String [] alphabetIndex = new String [] {"A", "B", "C", "D", "E", "F"};  
  private static final DistributedCrawler instance = new DistributedCrawler();
	
	private DistributedCrawler() { 
		
	}
	
	public static DistributedCrawler getInstance() {
		return instance;
	}
	
	public void init() {
		DistributedCrawler crawler = getInstance();
		crawler.hostNameMonitoring = "cis455.cis.upenn.edu";
		crawler.maxFileNum = Integer.MAX_VALUE; //TODO: change Later for Project
		crawler.robotMap = new HashMap<String, RobotsTxtInfo>();
		DistributedCrawler.bannedHosts = new HashSet<String>();
		DistributedCrawler.whiteList = new HashSet<String>();
		crawler.populateBannedHosts();
		crawler.populateWhiteList();
		crawler.lastCrawled = new HashMap<String, Date>();
		crawler.fileCount = new AtomicInteger(); //store number of downloaded files
		crawler.linksCrawled = new AtomicInteger(); //store number of links crawled
		crawler.http2xx = new AtomicInteger();
		crawler.http404 = new AtomicInteger();
		crawler.http3xx = new AtomicInteger();
		crawler.httpOther = new AtomicInteger();
		
		crawler.inFlightMessages = new AtomicInteger();
				
		try {
			crawler.hostMonitor = InetAddress.getByName(hostNameMonitoring);
		//	crawler.s = new DatagramSocket();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		} 
				
		storagePath = System.getProperty("user.dir")+"/DistributedStorage";
			
		File file = new File(storagePath);
		
		if(!file.exists() && file.mkdir()) {
			System.out.println("Directory created successfully");			
		}
		StorageServer.getInstance().init(storagePath);
		/*create URLDisk File */
		try {
			diskFile = new File(storagePath + "/URLDisk.txt");
			if(diskFile.exists()) { //incremental crawling
				int fileCount = StorageServer.getInstance().getFileCount();
				System.out.println("file count from disk: " + fileCount);
				crawler.fileCount.set(fileCount); //update fileCount
				crawler.frontier.clear(); //remove current Seed URLS									
			} else {
				if(diskFile.createNewFile()) {
					System.out.println("file created successfully");
				}
			}			
		} catch (IOException e) {
			
		}		
		
	}
	 
  public void setmaxDocSize(int size) {
	  getInstance().maxDocSize = size;
  }
  
  public void populateBannedHosts() {
	DistributedCrawler.bannedHosts.add("xxx.com"); 
	DistributedCrawler.bannedHosts.add("facebook.com");
	DistributedCrawler.bannedHosts.add("t.co"); 
	DistributedCrawler.bannedHosts.add("twitter.com"); 
	DistributedCrawler.bannedHosts.add("instagram.com"); 
	DistributedCrawler.bannedHosts.add("github.com"); 
  }
  
  public void populateWhiteList() {
		DistributedCrawler.whiteList.add("thehill.com"); 
		DistributedCrawler.whiteList.add("w3schools.com");
		DistributedCrawler.whiteList.add("pubmed.ncbi.nlm.nih.gov");
		DistributedCrawler.whiteList.add("en.wikipedia.org");
		DistributedCrawler.whiteList.add("stackexchange.com");
		DistributedCrawler.whiteList.add("stackoverflow.com");
		DistributedCrawler.whiteList.add("news.seas.upenn.edu");
		DistributedCrawler.whiteList.add("visitphilly.com");
	  }
  
  public int getMaxFileNum() {
	 return getInstance().maxFileNum;
  }
  public int getmaxDocSize() {
	  return getInstance().maxDocSize;
  }
  
  public void setHostName(String name) {
	  getInstance().hostNameMonitoring = name;
  }
  
  public void setmaxFileNum(int max) {
	  getInstance().maxFileNum = max;
  }
  
  public int getInFlightMessages() {
	  return getInstance().inFlightMessages.get();
  }
  public void incrementInflightMessages() {
	  getInstance().inFlightMessages.getAndIncrement();
  }
  
  public void decrementInflightMessages() {
	  getInstance().inFlightMessages.getAndDecrement();
  }
  
  public AtomicInteger getFileCount() {
	  return getInstance().fileCount;
  }
  
  public AtomicInteger getLinksCrawled() {
	  return getInstance().linksCrawled;
  }
  
  public AtomicInteger getHttp2xx() {
	  return getInstance().http2xx;
  }
  
  public AtomicInteger getHttp404() {
	  return getInstance().http404;
  }
  
  public AtomicInteger getHttp3xx() {
	  return getInstance().http3xx;
  }
  
  public AtomicInteger getHttpOther() {
	  return getInstance().httpOther;
  }
   
  public StorageServer getDB() {
	  return StorageServer.getInstance();
  }
  
  public URLFrontier getFrontier() {
	  return getInstance().frontier;
  }
   
  public HashMap<String, RobotsTxtInfo> getRobotMap(){
	  return getInstance().robotMap;
  }
  
  public InetAddress getHostMonitor() {
	  return getInstance().hostMonitor;
  }
  
  public HashMap<String, Date> getLastCrawled(){
	  return getInstance().lastCrawled;
  }
  
  public void initFrontier(String seed) {
	  getInstance().frontier = new URLFrontier(seed);
  }
   
  public synchronized void shutdown() { //TODO:
	
	  if(shutdownCalled) { //only proceed if this is the first time shutdown is called
		  return; 
	  }
	  shutdownCalled = true;
	  System.out.println("Called Shutdown, busy Waiting");
	  while(!(DistributedURLSpout.getActiveThreads() <= 0 && DistributedCrawlerBolt.getActiveThreads() <= 0 && DistributedDocumentParserBolt.getActiveThreads() <= 0
			  && HostSplitterBolt.getActiveThreads() <= 0 && DistributedURLFilterBolt.getActiveThreads() <= 0 && getInstance().inFlightMessages.get() <= 0) ){				  
			  }
	  getInstance().shutdown = true;
	  WorkerNode.setStatus("Finished");
	  //WorkerNode.
	  System.out.println("Set shutdown flag to: " + getInstance().getShutdown());
	  if(DistributedCrawler.getInstance().diskFile != null) {
    	  DistributedCrawler.getInstance().diskFile.delete(); //delete URLDIsk File
      }
	  
	  int index = WorkerNode.workerIndex;
	  File dir = new File(storagePath + "/upload");
	  dir.mkdir();
	  DistributedCrawler.getInstance().getDB().writetoFile(storagePath + "/upload/corpus" + alphabetIndex[index]); 
  }
  
  public boolean getShutdown() {
	  return getInstance().shutdown;
  }
  
  public void setup(String [] args) {
	  	 
	  DistributedCrawler.getInstance().init(); //dBDirectory
	  
	  try {
		  int maxDocSize = Integer.parseInt(args[0]) * 1000000;
		//  DistributedCrawler.getInstance().set
		  DistributedCrawler.getInstance().setmaxDocSize(maxDocSize);
	  } catch(NumberFormatException e) {
		  System.err.println("You need to provide the max document size as an Integer");
		  return;
	  }
	  		  
	  if(args.length == 2) {
		  try {
			  int maxFileNum = Integer.parseInt(args[1]); //4th argument could be max file num or monitoring hostname
			  DistributedCrawler.getInstance().setmaxFileNum(maxFileNum);
		  } catch(NumberFormatException e) {
			  DistributedCrawler.getInstance().setHostName(args[1]);
		  }
	  }
	  	  
	  if(args.length == 3) {
		  DistributedCrawler.getInstance().setHostName(args[2]);
		  try {
			  int maxFileNum = Integer.parseInt(args[1]); 
			  DistributedCrawler.getInstance().setmaxFileNum(maxFileNum);
		  } catch(NumberFormatException e) {			
			  System.err.println("Invalid Max Number of Files");
		  }
	  }	   	 
  }
  
  public Topology createTopology() {
	  /*Create Topology  */
	  //Config config = new Config();
	  
      DistributedURLSpout spout = new DistributedURLSpout();
      DistributedCrawlerBolt crawlerBolt = new DistributedCrawlerBolt();
      DistributedDocumentParserBolt docParserBolt = new DistributedDocumentParserBolt();
      HostSplitterBolt hostBolt = new HostSplitterBolt();
      DistributedURLFilterBolt filterBolt = new DistributedURLFilterBolt();

      // wordSpout ==> countBolt ==> MongoInsertBolt
      TopologyBuilder builder = new TopologyBuilder();

      // Only one source ("spout") for the urls
      builder.setSpout(URL_SPOUT, spout, 1);
      
      // Four parallel crawler spiders, each of which gets specific urls based on hostname
      builder.setBolt(CRAWLER_BOLT, crawlerBolt, 5).fieldsGrouping(URL_SPOUT, new Fields("host")); //group based on hostname
     // builder.setBolt(CRAWLER_BOLT, crawlerBolt, 1).shuffleGrouping(URL_SPOUT); //no grouping, for testing purposes.
      
      // A single docParser bolt to store documents and extract URLS
      builder.setBolt(DOCPARSER_BOLT, docParserBolt, 5).shuffleGrouping(CRAWLER_BOLT);
      
   // A single host splitter bolt to route URLS to the correct worker
      builder.setBolt(HOST_BOLT, hostBolt, 1).shuffleGrouping(DOCPARSER_BOLT); 
      
      //Finally a URL Filter Bolt to add items back to the queue
      builder.setBolt(URLFILTER_BOLT, filterBolt, 1).shuffleGrouping(HOST_BOLT);

     // LocalCluster cluster = new LocalCluster();
      Topology topo = builder.createTopology();

      ObjectMapper mapper = new ObjectMapper();
		try {
			String str = mapper.writeValueAsString(topo);
			
			System.out.println("The StormLite topology is:\n" + str);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		return topo;
  }
  
  
  public void start() {
	  
	  while(!DistributedCrawler.getInstance().getShutdown()) {
    	  
      }
	  System.out.println("Shutting Down Crawler Cluster");
	  System.out.println("Writing to file"); 
      DistributedCrawler.getInstance().getDB().writetoFile("corpus_html.txt");     
      
  	}  
}
