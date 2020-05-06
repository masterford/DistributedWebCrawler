package edu.upenn.cis455.crawler;

/**
*@author Ransford Antwi
*/

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;



import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.helper.W3CDom;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.storage.ChannelStorage;
import edu.upenn.cis455.storage.DocVal;
import edu.upenn.cis455.xpathengine.*;

public class ChannelMatchingBolt implements IRichBolt{
	static Logger log = Logger.getLogger(ChannelMatchingBolt.class);
	
	Fields schema = new Fields("url");
	
	private String [] XPaths; 
    
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
    /**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    private XPathEngineImpl engine;
    private HashMap<String, ArrayList<ChannelStorage>> xPathChannels; //used to match XPath to list of Channels
    private static AtomicInteger activeThreads;
    
    public ChannelMatchingBolt() {
    	ChannelMatchingBolt.activeThreads = new AtomicInteger();
    	
    	HashMap<String, ChannelStorage> allChannels = XPathCrawler.getInstance().getDB().getAllChannels();
    	if(allChannels == null) {
    		allChannels = new HashMap<String, ChannelStorage>();
    	}
    	xPathChannels = new HashMap<String, ArrayList<ChannelStorage>>();
    	XPaths = new String[allChannels.size()];
    	int i = 0;
    	for(String channel : allChannels.keySet()) {
    		ChannelStorage channelObject = allChannels.get(channel);
    		XPaths[i] = channelObject.getXPath();
    		System.out.println(channelObject.getXPath());
    		channelObject.clearMatches();
    		ArrayList<ChannelStorage> list = xPathChannels.getOrDefault(channelObject.getXPath(), new ArrayList<ChannelStorage>());
    		list.add(channelObject);
    		xPathChannels.put(channelObject.getXPath(), list);
    		i++;
    	}
    	 engine = new XPathEngineImpl();
    	 engine.setXPaths(XPaths);
    }
    
    public static int getActiveThreads() {
    	return ChannelMatchingBolt.activeThreads.get();
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    
    

    /**
     * Process a tuple received from the stream, Instantiates the XPath engine for the given document and checks if any of the defined channels 
     * in the DB match the given document
     */
    @Override
    public void execute(Tuple input) {    	
    	XPathCrawler.getInstance().decrementInflightMessages();
    	//Check if maxFileCount reached, if so send shutdown
    	
    	String url = input.getStringByField("url");
    	XPathCrawler.getInstance().incrementInflightMessages();
    	collector.emit(new Values<Object>(url));
    	/*
    	if(XPathCrawler.getInstance().getDB().getAllChannels() == null) { //no channels in DB   		
    		return;
    	}
    	
    	ChannelMatchingBolt.activeThreads.getAndIncrement();
    	   	  	
    	
    	DocVal docVal = (DocVal) input.getObjectByField("document");
    	if(url == null || docVal == null || input.getStringByField("toMatch").equals("false")) {
    		ChannelMatchingBolt.activeThreads.getAndDecrement();
    		return;
    	}
    	//Parse doc to w3c doc
    	Document doc = null;    
    	if(docVal.getContentType().contains("text/html") || docVal.getContentType().endsWith(".html")) {
    		W3CDom w3cDom = new W3CDom();
    		String html = new String(docVal.getBody()); 
    		org.jsoup.nodes.Document jsoup =  Jsoup.parse(html);
    		jsoup.setBaseUri(url);
    		doc = w3cDom.fromJsoup(jsoup);
    	}else { //xml  		
    		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        	DocumentBuilder dBuilder;   	   		
    		
    		try {
    			dBuilder = dbFactory.newDocumentBuilder();
    			InputSource is = new InputSource(new StringReader(docVal.getBody()));
    			doc = dBuilder.parse(is);
    		} catch (SAXException | IOException | ParserConfigurationException e) {
    			e.printStackTrace();
    			ChannelMatchingBolt.activeThreads.getAndDecrement();
    			return;
    		}    		
    	}
     
    	if(doc == null) {
    		ChannelMatchingBolt.activeThreads.getAndDecrement();
    		return;
    	}
    	
    	//System.out.println("DOC Node: " + doc.getNodeName());
    	boolean[] result = engine.evaluate(doc);
    	if(result == null) {
    		ChannelMatchingBolt.activeThreads.getAndDecrement();
    		return;
    	}
 
    	for (int i = 0; i < result.length; i++) {
    		if(result[i]) { //found matching XPath
    			System.out.println("Found a match: " + url);
    			ArrayList<ChannelStorage> channels = xPathChannels.get(XPaths[i]); //get all channels with this XPath
    			for (ChannelStorage storage : channels) {
    				storage.addDocumentURLs(url); //add matching doc url
    				XPathCrawler.getInstance().getDB().addChannelInfo(storage.getName(), storage); //re-add back to database
    			}   			
    		}
    	}
    	ChannelMatchingBolt.activeThreads.getAndDecrement(); */
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    	
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


