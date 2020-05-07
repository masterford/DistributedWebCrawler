package edu.upenn.cis455.crawler;

import edu.upenn.cis455.storage.StorageServer;
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

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.crawler.info.*;

/** (MS1, MS2) The main class of the crawler.
  */
public class XPathCrawler {
  private int maxDocSize;
  private int maxFileNum;
  private String hostNameMonitoring;
  private HashMap<String, RobotsTxtInfo> robotMap; //caches the results of robots.txt for each domain
  public static HashSet<String> bannedHosts; //set to maintain banned hosts, normalized
 // private HashSet<String> seenContent; //set to maintain seen content
  private HashMap<String, Date> lastCrawled; //data structure to keep track of the last time a hostname server was crawled
  private InetAddress hostMonitor;
  private AtomicInteger linksCrawled;
  private AtomicInteger fileCount;
  private AtomicInteger inFlightMessages; //used to keep track of messages being routed so that we don't shutdown prematurely
  private URLFrontier frontier;
  public File diskFile;
  public static final int FRONTIER_BUFFER_SIZE = 1000; //number of URLS to keep in memory
  private boolean shutdownCalled = false; //flag to keep track of whether the shutdown method has already been called
  private volatile boolean shutdown = false; //flag to keep track of whether to shutdown or not
  
  private static final String URL_SPOUT = "URL_SPOUT";
  private static final String CRAWLER_BOLT = "CRAWLER_BOLT";
  private static final String DOCPARSER_BOLT = "DOCPARSER_BOLT";
  //private static final String CHANNEL_BOLT = "CHANNEL_BOLT";
  private static final String URLFILTER_BOLT = "URLFILTER_BOLT";
  
  private static final XPathCrawler instance = new XPathCrawler();
	
	private XPathCrawler() { 
		
	}
	
	public static XPathCrawler getInstance() {
		return instance;
	}
	
	public void init(String startURL, String dBDirectory) {
		XPathCrawler crawler = getInstance();
		crawler.hostNameMonitoring = "cis455.cis.upenn.edu";
		crawler.maxFileNum = Integer.MAX_VALUE; //TODO: change Later for Project
		crawler.robotMap = new HashMap<String, RobotsTxtInfo>();
		XPathCrawler.bannedHosts = new HashSet<String>();
		crawler.populateBannedHosts();
		crawler.lastCrawled = new HashMap<String, Date>();
		crawler.fileCount = new AtomicInteger(); //store number of downloaded files
		crawler.inFlightMessages = new AtomicInteger();
		crawler.linksCrawled = new AtomicInteger(); //store number of links crawled
		crawler.frontier = new URLFrontier(startURL);
		/*Add more seed URLS */
		//crawler.frontier.enqueue("https://en.wikipedia.org/wiki/File_system");
		//crawler.frontier.enqueue("https://www.reddit.com/");
		//crawler.frontier.enqueue("https://crawltest.cis.upenn.edu/");//TODO: testing, remove later
		//crawler.frontier.enqueue("https://www.cnn.com/");//: testing, remove later
		try {
			crawler.hostMonitor = InetAddress.getByName(hostNameMonitoring);
		//	crawler.s = new DatagramSocket();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		} 		
		
		StorageServer.getInstance().init(dBDirectory); //initialize BDB
		/*create URLDisk File */
		try {		
			diskFile = new File("URLDisk.txt");
			if(diskFile.exists()) { //incremental crawling
				int fileCount = StorageServer.getInstance().getFileCount();
				crawler.fileCount.set(fileCount); //update fileCount
				crawler.frontier.clear(); //remove current Seed URLS
			} else {
				if(diskFile.createNewFile()) {
					System.out.println("file created successfully");
				}
			}			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	 
  public void setmaxDocSize(int size) {
	  getInstance().maxDocSize = size;
  }
  
  public void populateBannedHosts() {
	XPathCrawler.bannedHosts.add("xxx.com"); //TODO: populate
  }
  
  public int getMaxFileNum() {
	 return getInstance().maxFileNum;
  }
  
  public AtomicInteger getLinksCrawled() {
	  return getInstance().linksCrawled;
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
   
  public synchronized void shutdown() { //TODO:
	
	  if(shutdownCalled) { //only proceed if this is the first time shutdown is called
		  return; 
	  }
	  shutdownCalled = true;
	  System.out.println("Called Shutdown, busy Waiting");
	  while(!(URLSpout.getActiveThreads() <= 0 && CrawlerBolt.getActiveThreads() <= 0 && DocumentParserBolt.getActiveThreads() <= 0
			  && URLFilterBolt.getActiveThreads() <= 0 && getInstance().inFlightMessages.get() <= 0) ){
				  //busy wait till all spouts and bolts are idle	  
		  			//System.out.println("URL active threads: " + URLSpout.getActiveThreads());
		  			//System.out.println("CrawlerBolt active threads: " + CrawlerBolt.getActiveThreads());
		  			//System.out.println("DocumentParser active threads: " + DocumentParserBolt.getActiveThreads());
		  			//System.out.println("URLFilterBolt: " + URLFilterBolt.getActiveThreads());
		  			//System.out.println("in flight messages: " + getInstance().inFlightMessages.get());
			  }
	  getInstance().shutdown = true;	  
	  System.out.println("Set shutdown flag to: " + getInstance().getShutdown());
  }
  
  public boolean getShutdown() {
	  return getInstance().shutdown;
  }
  
  public static void main(String args[])
  {
	  if (args.length < 3) {
	      System.err.println("You need to provide the start URL, database directory and max document size as command line arguments");
	      System.exit(1);
	    }
	  
	  /* initialize command line args */
	 // XPathCrawler crawler = new XPathCrawler();
	  	  
	  XPathCrawler.getInstance().init(args[0], args[1]);
	  try {
		  int maxDocSize = Integer.parseInt(args[2]) * 1000000;
		//  XPathCrawler.getInstance().set
		  XPathCrawler.getInstance().setmaxDocSize(maxDocSize);
	  } catch(NumberFormatException e) {
		  System.err.println("You need to provide the max document size as an Integer");
		  System.exit(1);
	  }
	  		  
	  if(args.length == 4) {
		  try {
			  int maxFileNum = Integer.parseInt(args[3]); //4th argument could be max file num or monitoring hostname
			  XPathCrawler.getInstance().setmaxFileNum(maxFileNum);
		  } catch(NumberFormatException e) {
			  XPathCrawler.getInstance().setHostName(args[3]);
		  }
	  }
	  	  
	  if(args.length == 5) {
		  XPathCrawler.getInstance().setHostName(args[4]);
		  try {
			  int maxFileNum = Integer.parseInt(args[3]); 
			  XPathCrawler.getInstance().setmaxFileNum(maxFileNum);
		  } catch(NumberFormatException e) {			
			  System.err.println("Invalid Max Number of Files");
		  }
	  }	  	 	 	  
	  
	  
	  
	  /*Create Topology  */
	  Config config = new Config();

      URLSpout spout = new URLSpout();
      CrawlerBolt crawlerBolt = new CrawlerBolt();
      DocumentParserBolt docParserBolt = new DocumentParserBolt();
      //ChannelMatchingBolt channelBolt = new ChannelMatchingBolt();
      URLFilterBolt filterBolt = new URLFilterBolt();

      // wordSpout ==> countBolt ==> MongoInsertBolt
      TopologyBuilder builder = new TopologyBuilder();

      // Only one source ("spout") for the urls
      builder.setSpout(URL_SPOUT, spout, 1);
      
      // Four parallel crawler spiders, each of which gets specific urls based on hostname
      builder.setBolt(CRAWLER_BOLT, crawlerBolt, 4).fieldsGrouping(URL_SPOUT, new Fields("host")); //group based on hostname
     // builder.setBolt(CRAWLER_BOLT, crawlerBolt, 1).shuffleGrouping(URL_SPOUT); //no grouping, for testing purposes.
      
      // A single docParser bolt to store documents and extract URLS
      builder.setBolt(DOCPARSER_BOLT, docParserBolt, 4).shuffleGrouping(CRAWLER_BOLT);
      
   // A single channel matching bolt to match channels to documents
    //  builder.setBolt(CHANNEL_BOLT, channelBolt, 4).shuffleGrouping(DOCPARSER_BOLT); //TODO: replace
      
      //Finally a URL Filter Bolt to add items back to the queue
      builder.setBolt(URLFILTER_BOLT, filterBolt, 1).shuffleGrouping(DOCPARSER_BOLT);

      LocalCluster cluster = new LocalCluster();
      Topology topo = builder.createTopology();

      ObjectMapper mapper = new ObjectMapper();
		try {
			String str = mapper.writeValueAsString(topo);
			
			System.out.println("The StormLite topology is:\n" + str);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
     
      
      cluster.submitTopology("DistributedCrawler", config, 
      		builder.createTopology());
    
      System.out.println("current file count: " + XPathCrawler.getInstance().getFileCount());
      while(!XPathCrawler.getInstance().getShutdown()) {
    	  
      } 
      System.out.println("Shutting Down Cluster");
      
      cluster.killTopology("DistributedCrawler");
      cluster.shutdown();       
      System.out.println("Writing to file"); 
      XPathCrawler.getInstance().getDB().writetoFile("CorpusD_0.txt");
      
      if(XPathCrawler.getInstance().diskFile != null) {
    	  XPathCrawler.getInstance().diskFile.delete(); //delete URLDIsk File
      } 
      System.out.println("Uploading to S3");
      XPathCrawler.getInstance().getDB().writetoS3("CorpusD_0.txt");
           
     // StorageServer.getInstance().close();
      System.exit(0);
  }
  	
}
