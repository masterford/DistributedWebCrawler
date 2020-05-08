/* *
 * @author: Ransford Antwi
 */

package edu.upenn.cis455.crawler.distributed;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

public class DistributedURLFilterBolt implements IRichBolt {

	
	static Logger log = Logger.getLogger(DistributedURLFilterBolt.class);
	
	Fields schema = new Fields(); //not emitting anything
	
    
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
    /**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    private static AtomicInteger activeThreads;
    private final int MAX_URL_LENGTH = 100; //max length of a URL
    private final int MAX_DEPTH = 5;
   // private final int FRONTIER_BUFFER_SIZE = 1;
    
    BufferedWriter bw;
    public DistributedURLFilterBolt() {
    	activeThreads = new AtomicInteger();
    }
    
    public static int getActiveThreads() {
    	return DistributedURLFilterBolt.activeThreads.get();
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
			bw = new BufferedWriter(new FileWriter(DistributedCrawler.storagePath + "/URLDisk.txt", true));
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
        
    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public void execute(Tuple input) {
    	DistributedCrawler.getInstance().decrementInflightMessages(); //message has been routed to final destination
    	DistributedURLFilterBolt.activeThreads.getAndIncrement();
    	//Check if maxFileCount reached, if so send shutdown
    	String url = input.getStringByField("url");
    	if (url == null || url.length() > MAX_URL_LENGTH) {
    		DistributedURLFilterBolt.activeThreads.getAndDecrement();
    		return;
    	}
    	try {
			URL urlObject = new URL(url);
			String path = urlObject.getPath();
			String [] depth = path.split("/");
			if(depth.length > MAX_DEPTH) {
				DistributedURLFilterBolt.activeThreads.getAndDecrement();
	    		return;
			}
			String hostName = urlObject.getHost();
			if(DistributedCrawler.bannedHosts.contains(hostName) || ( (hostName.contains("wiki") || hostName.contains("wiktionary") || hostName.contains("stackoverflow")) && !hostName.startsWith("en"))) { //filter out banned hosts
				DistributedURLFilterBolt.activeThreads.getAndDecrement();
				return;				
			}
		} catch (MalformedURLException e) {			
			DistributedURLFilterBolt.activeThreads.getAndDecrement();
			return;
		}
    	//System.out.println(getActiveThreads());
    	//HashSet<String> seenURLs = (HashSet<String>) DistributedCrawler.getInstance().getSeenURLs();
    	if(DistributedCrawler.getInstance().getDB().addURL(url) == 0) { //if not 0 it means this URL is already seen
    		//seenURLs.add(url);
    		//if(DistributedCrawler.getInstance().getFrontier().getSize() >= DistributedCrawler.FRONTIER_BUFFER_SIZE) { //write to file
			try {
				//System.out.println("writing to file. size is: " + DistributedCrawler.getInstance().getFrontier().getSize());
				bw.write(url + "\n");
				bw.flush();				
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Problem writing to URL disk file");
				DistributedCrawler.getInstance().getFrontier().enqueue(url);
			}    		  	
    	}
    	DistributedURLFilterBolt.activeThreads.getAndDecrement();
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    	if(bw != null) {
    		try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	//wordCounter.clear();
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
 }
