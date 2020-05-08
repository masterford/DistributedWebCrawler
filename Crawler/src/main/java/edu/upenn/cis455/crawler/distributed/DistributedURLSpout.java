/* *
 * @author: Ransford Antwi
 */

package edu.upenn.cis455.crawler.distributed;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.URLInfo;

public class DistributedURLSpout implements IRichSpout {
	static Logger log = Logger.getLogger(DistributedURLSpout.class);

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordSpout, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    /**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
    TopologyContext context;
	SpoutOutputCollector collector;
	BufferedReader reader;
	//private final int FRONTIER_BUFFER_SIZE = 1;
	private static AtomicInteger activeThreads; //used to check whether this sput is idle
					

    public DistributedURLSpout() {
    	DistributedURLSpout.activeThreads = new AtomicInteger();
    	log.debug("Starting URL spout");
    }
    
    public static int getActiveThreads() {
    	return DistributedURLSpout.activeThreads.get();
    }

    /**
     * Initializes the instance of the spout (note that there can be multiple
     * objects instantiated)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;      
        try {
			reader = new BufferedReader(new FileReader(DistributedCrawler.storagePath + "/URLDisk.txt"));
			int num_lines = DistributedCrawler.getInstance().getFileCount().get();
			for (int i = 0; i < num_lines; i++) {
				reader.readLine();  //set reader to latest line
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
      //  log.debug(getExecutorId() + " Starting URL Spout");					
    }

    /**
     * Shut down the spout
     */
    @Override
    public void close() {
    	try {
    		if(reader != null) {
    			reader.close();
    		}		
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    	    
    /**
     * The real work happens here, in incremental fashion.  We process and output
     * the next item(s).  They get fed to the collector, which routes them
     * to targets
     * Emit host name and url, later in the crawler bolt, we will group by host such that only one bolt task handles a host
     */
    @Override
    public void nextTuple() {
    	int fileCount = DistributedCrawler.getInstance().getFileCount().get();
    	if(DistributedCrawler.getInstance().getFrontier().isEmpty()) { //refill from disk
    		DistributedURLSpout.activeThreads.getAndIncrement();
    		//System.out.println("refill time");
    		String url;
			try {
				url = reader.readLine();
				if(url != null) {
					//System.out.println("enqueue: " + url);
	    			DistributedCrawler.getInstance().getFrontier().enqueue(url);
	    			while(url != null) {
	    				//System.out.println("reading from file. Size is: " + DistributedCrawler.getInstance().getFrontier().getSize());
	    				url = reader.readLine();
	    				if(url == null) {
	    					break;
	    				}
	    				//System.out.println("enqueue2: " + url);
	    				DistributedCrawler.getInstance().getFrontier().enqueue(url);
	    				if(DistributedCrawler.getInstance().getFrontier().getSize() >= DistributedCrawler.FRONTIER_BUFFER_SIZE) { //buffe
	    					break;
	    				}
	    			}
	    		}
			} catch (IOException e) {
				System.out.println("unable to read from URLDisk file");
			}
			finally {
				DistributedURLSpout.activeThreads.getAndDecrement();
			}			
    	}
    	
    	if((DistributedCrawler.getInstance().getFrontier().isEmpty() && WorkerNode.receivedURLs.isEmpty())|| fileCount >= DistributedCrawler.getInstance().getMaxFileNum() || DistributedCrawler.getInstance().getShutdown()) { // Handle Shutdown
    		if(DistributedURLSpout.getActiveThreads() <= 0 && DistributedCrawlerBolt.getActiveThreads() <= 0 && DistributedDocumentParserBolt.getActiveThreads() <= 0 && DistributedURLFilterBolt.getActiveThreads() <= 0 && 
    			HostSplitterBolt.getActiveThreads() <= 0 && DistributedCrawler.getInstance().getInFlightMessages() <= 0) {
    			DistributedCrawler.getInstance().shutdown(); //call shutdown    			
    			return;
    		} else {
    			return; //just return and don't emit anything
    		}
    		
    	} else {
    		DistributedURLSpout.activeThreads.getAndIncrement(); //isIdle is now 1, hence this thread is not idle
        	String url = DistributedCrawler.getInstance().getFrontier().dequeue();
        	if(url.startsWith("http://")) {
        		URLInfo urlInfo = new URLInfo(url);	
        		if(urlInfo.getHostName() == null) {
        			DistributedURLSpout.activeThreads.decrementAndGet();
        			return;
        		}
        		String portString = ":" + Integer.toString(urlInfo.getPortNo());
    			  if(!url.contains(portString)) { //make sure URL is in the form http://xyz.com:80/
    				  StringBuilder newURL = new StringBuilder(url);
    	  			  int index = 7 + urlInfo.getHostName().length();
    	  			  newURL.insert(index, portString);
    				  url = newURL.toString();
    			  }
    			  
            	//log.debug(getExecutorId() + " emitting " + url);
    	        this.collector.emit(new Values<Object>(urlInfo.getHostName(), url, "http"));
    	        
    	        DistributedCrawler.getInstance().incrementInflightMessages();  //signals a message is currently being routed
        	}else {
        		try {
					URL httpsUrl = new URL(url); //emit https url
					int port = httpsUrl.getPort() == -1 ? 443 : httpsUrl.getPort();
					String portString = ":" + Integer.toString(port);
					if(!url.contains(portString)) { //make sure URL is in the form http://xyz.com:443/
	    				  StringBuilder newURL = new StringBuilder(url);
	    	  			  int index = 8 + httpsUrl.getHost().length();
	    	  			  newURL.insert(index, portString);
	    				  url = newURL.toString();
	    			  }
					//log.debug(getExecutorId() + " emitting " + url);
	    	        this.collector.emit(new Values<Object>(httpsUrl.getHost(), url, "https"));
	    	        DistributedCrawler.getInstance().incrementInflightMessages();  //signals a message is currently being routed
				} catch (MalformedURLException e) {
					e.printStackTrace();
				} finally {
					DistributedURLSpout.activeThreads.decrementAndGet();
				}
        		return;
        	}
        	
        	DistributedURLSpout.activeThreads.decrementAndGet(); //isIdle is now 0 hence this thread is idle
    	}   	
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("host", "url", "protocol"));
    }


	@Override
	public String getExecutorId() {
		
		return executorId;
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}
}
