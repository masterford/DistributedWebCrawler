/* *
 * @author: Ransford Antwi
 */

package edu.upenn.cis455.crawler;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;
import org.apache.tika.language.LanguageIdentifier;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DocVal;

public class DocumentParserBolt implements IRichBolt{
	static Logger log = Logger.getLogger(DocumentParserBolt.class);
	
	Fields schema = new Fields("url", "document", "toMatch");
	
    
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
    /**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    private final int HTTPS_PORT = 443; //default HTTPS port
    private static AtomicInteger activeThreads;
    
    public DocumentParserBolt() {
    	DocumentParserBolt.activeThreads = new AtomicInteger();
    }
    
    public static int getActiveThreads() {
    	return DocumentParserBolt.activeThreads.get();
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        
    }
    
    /*Creates a normalized URL of the form protocol // hostname:portNo/filepath. e.g: http://crawltest.cis.upenn.edu:80/foo.html
     * @param oldURL : The current url/page where we found the new link.
     * @param URLInfo: URLInfo object for current URL
     * @param redirectURL: New URL to visit, could be either relative or absolute
     *  */
    private String normalize(String oldURL, URLInfo urlInfo, String redirectURL) throws Exception {
  	  StringBuilder newURL;
  	  if(redirectURL.startsWith("http")) { //already absolute
  		   newURL = new StringBuilder(redirectURL);
  		   //append port number
  		   String portString = ":" + Integer.toString(urlInfo.getPortNo());
  		   URLInfo info = new URLInfo(redirectURL);
  		  if(!redirectURL.contains(portString)){ //append port
  			   //check if host names are equivalent
  			   if(info != null && info.getHostName() != null && info.getHostName().equals(urlInfo.getHostName())) {
  				   int index = redirectURL.startsWith("https://") ? 8 : 7;
  				   index += urlInfo.getHostName().length();
  				   newURL.insert(index, portString);
  			   } else {
  				   //append default port
  				   String defaultPortString = ":80";
  				   int index = 7 + info.getHostName().length();
  				   newURL.insert(index, defaultPortString);
  			   }			   
  		   }		   		  	   
  	  }else { //relative URL
  		   String temp = oldURL.substring(0, oldURL.lastIndexOf("/"));
  		   newURL = new StringBuilder(temp);
  		   newURL.append(redirectURL);
  	  } 
  	  return newURL.toString();
    }
    
    private String normalizeHttps(String oldURL, URL url, String redirectURL) throws Exception  {
  	  StringBuilder newURL;
  	  if(redirectURL.startsWith("http")) { //already absolute
  		   newURL = new StringBuilder(redirectURL);
  		   //append port number  		   
  		   int port = url.getPort() == -1 ? HTTPS_PORT : url.getPort();
  		   String portString = ":" + Integer.toString(port);
  		   try {
			URL normalizeURL = new URL(redirectURL);
			if(!redirectURL.contains(portString)){ //append port
				if(normalizeURL.getHost() != null && normalizeURL.getHost().equals(url.getHost())){		   
	  			   int index = 8 + url.getHost().length();
	  			   newURL.insert(index, portString);
	  		   } else {
  				   int index = 8 + normalizeURL.getHost().length();
  				   newURL.insert(index, portString);
	  		   }
			}
		} catch (MalformedURLException e) {
			return redirectURL;
		}
  		   		   		  	   
  	  }else { //relative URL
  		// System.out.println("oldurl: " + oldURL);
  		   String temp = oldURL.substring(0, oldURL.lastIndexOf("/"));
  		   newURL = new StringBuilder(temp);
  		   newURL.append(redirectURL);
  		// System.out.println("new url" + newURL.toString());
  	  } 
  	  return newURL.toString();
    }

    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public void execute(Tuple input) {
    	XPathCrawler.getInstance().decrementInflightMessages();
    	//Check if maxFileCount reached, if so send shutdown
    	
    	DocumentParserBolt.activeThreads.getAndIncrement();
    	String url = input.getStringByField("url");
    	DocVal doc = (DocVal) input.getObjectByField("document");
    	//check duplicate content
    	if(doc == null || doc.getBody() == null) {
    		XPathCrawler.getInstance().incrementInflightMessages();
        	collector.emit(new Values<Object>(url, null, "false"));
    		DocumentParserBolt.activeThreads.getAndDecrement();
    		return;
    	}
    	//Check for duplicate content
    	String hash = DigestUtils.md5Hex(doc.getBody());
    	if(XPathCrawler.getInstance().getDB().addContentHash(hash) != 0) { //if not 0 it means this content is already seen    	
    		DocumentParserBolt.activeThreads.getAndDecrement();
    		return;
    	}
    	//check language
    	    	
    	if(input.getStringByField("toStore") != null && input.getStringByField("toStore").equals("true")) {
    		XPathCrawler.getInstance().getDB().addDocInfo(url, doc); //store document in DB
    		XPathCrawler.getInstance().getFileCount().getAndIncrement(); //increment file count
    		//System.out.println("Current file count: " + XPathCrawler.getInstance().getFileCount().get());
    	}
    
    	XPathCrawler.getInstance().incrementInflightMessages();
    	collector.emit(new Values<Object>(url, doc, "true"));
    	
    	if(doc.getContentType() != null && (doc.getContentType().contains("text/html") || doc.getContentType().endsWith(".html"))) { //parse html doc
    		URL httpsUrl = null;
    		try {
    			httpsUrl = new URL(url);
    		} catch (MalformedURLException e) {
    			e.printStackTrace();
    			DocumentParserBolt.activeThreads.decrementAndGet();
    			return;
    		}
    		String html = new String(doc.getBody());     		
    		Document jdoc = Jsoup.parse(html);
    		LanguageIdentifier identifier = new LanguageIdentifier(jdoc.text());    		
    		if(!identifier.getLanguage().equals("en") && identifier.isReasonablyCertain()) { //skip non english content
    			System.out.println("language: " + identifier.getLanguage() + "url: " + url);
    			DocumentParserBolt.activeThreads.getAndDecrement();
    			return;
    		}       	
    		jdoc.setBaseUri(url);   		
    		String language = jdoc.attr("lang");
    		if(!language.isEmpty() && !language.equalsIgnoreCase("en")) { //not english    
    			System.out.println("lang: " + language);
    			DocumentParserBolt.activeThreads.getAndDecrement();
    			return;
    		}
    		Elements links = jdoc.select("a[href]");
    		
    		for(Element link : links) {
    			try {
    				String absolute = link.attr("abs:href");
        			String normalized = null;
        			//if(url.equals("https://crawltest.cis.upenn.edu:443/marie/tpc")) {
        				//System.out.println("absolute: " + absolute);
        				//System.out.println("Base url: " + jdoc.baseUri());
        			//}       			
        			if(absolute.startsWith("http://")) {
        				URLInfo info = new URLInfo(absolute);
        				normalized = normalize(url,  info, absolute);
        			}else {
        				normalized = normalizeHttps(url,  httpsUrl, absolute);
        			}			
        			collector.emit(new Values<Object>(normalized, doc, "false"));
        			XPathCrawler.getInstance().incrementInflightMessages();
    			}catch(Exception e) {
    				continue;
    			}   			
    		} 
    	}
    	DocumentParserBolt.activeThreads.getAndDecrement();
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
