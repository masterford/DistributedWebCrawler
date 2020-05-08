/* *
 * @author: Ransford Antwi
 */

package edu.upenn.cis455.crawler.distributed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
//import java.net.ConnectException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLException;

import org.apache.log4j.Logger;
import org.apache.tika.io.IOUtils;

import com.sleepycat.je.DatabaseException;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DocVal;
import edu.upenn.cis455.storage.StorageServer;


public class DistributedCrawlerBolt implements IRichBolt {
	static Logger log = Logger.getLogger(DistributedCrawlerBolt.class);
	
	Fields schema = new Fields("url", "document", "toStore"); //TODO:
	 
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
    /**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    /*Private Class variables to aid with crawling */
    private final String USER_AGENT_HEADER = "User-Agent: cis455crawler\r\n";
    private final String USER_AGENT = "cis455crawler";
    private final String CLRF = "\r\n";
    private final int HTTPS_PORT = 443; //default HTTPS port
    private final int SOCKET_TIMEOUT = 5000;
    private final int READ_TIMEOUT = 10000;
    private InetAddress hostMonitor;
    private DatagramSocket s;
    private static AtomicInteger activeThreads;
    
    public DistributedCrawlerBolt() {
    	DistributedCrawlerBolt.activeThreads = new AtomicInteger();
    	hostMonitor = DistributedCrawler.getInstance().getHostMonitor();
    	try {
			s = new DatagramSocket();
		} catch (SocketException e) {
			e.printStackTrace();
		}
    }
    
    public static int getActiveThreads() {
    	return DistributedCrawlerBolt.activeThreads.get();
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    private void parseHeader(String line, HashMap<String, String> headerMap) {
		String[] split = line.split(":", 2); //split once
		if(split.length == 2) {
			headerMap.put(split[0].toLowerCase(), split[1].trim()); //lowercase
		}
	}
  
    /*Parses the HTTP response
     * @Param responseHeaders: hashmap of http headers that will be populated
     * @Param inputStream: The socket input Stream
     * @Param requestType: used to check whether we are parsing a HEAD or GET request. 0 is for Head request and 1 is for GET request
     *   */
  private String parseResponse(HashMap<String, String> responseHeaders, InputStream inputStream, int requestType) throws IOException {	   
		//InputStream inputStream = socket.getInputStream();
		int b;
		StringBuilder requestBuilder = new StringBuilder();
		Reader reader = new InputStreamReader(inputStream, StandardCharsets.US_ASCII);		
		String line = "tth"; 
		//while((b = inputStream.read()) != -1) {
		while((b = reader.read()) != -1) {
			char c = (char) b;
			requestBuilder.append(Character.toString(c));											
			if ( c == '\n') { //end of line	/home/cis455/git/HW2/berkeleydb
				line = requestBuilder.toString();
				if(line.startsWith("HTTP")) {
					String [] result = line.split(" ");
					responseHeaders.put("Code", result[1]);
				}				 
				 parseHeader(line, responseHeaders);
				 requestBuilder.delete(0, line.length()); //flush					 						 							  							
			}
			if(line.isBlank()) { //end of stream
				if(responseHeaders.get("content-length") != null && requestType == 1) { //body for get request	
					requestBuilder = new StringBuilder();
					 int size =  Integer.parseInt(responseHeaders.get("content-length").trim());
					 int count = 0;
					 while(count < size) {
						 b = reader.read();
						 requestBuilder.append((char) b);
						 count++;
					}
				 } 
				break;
			}
		}
		return requestBuilder.toString();
  }
  
  /*Parses the HTTPS body
   * @Param responseHeaders: hashmap of http headers that will be populated
   * @Param inputStream: The socket input Stream
   * @Param requestType: used to check whether we are parsing a HEAD or GET request. 0 is for Head request and 1 is for GET request
   *   */
private String parseHTTPSBody(long contentLength, InputStream inputStream) throws IOException {	   
	
	int b;
	StringBuilder requestBuilder = new StringBuilder();
	Reader reader = new InputStreamReader(inputStream, StandardCharsets.US_ASCII);				
	 int count = 0;
	 while(count < contentLength) {
		 b = reader.read();
		 requestBuilder.append((char) b);
		 count++;
	}
	return requestBuilder.toString();
}
  
  /*Checks Robots.txt to see if we can crawl given file path  */
  private boolean canCrawl(RobotsTxtInfo robotInfo, String filePath) {  
		 if(robotInfo.containsUserAgent(USER_AGENT)) { //only care about directives for this user agent
			 ArrayList<String> disallowedLinks = robotInfo.getDisallowedLinks(USER_AGENT);
			 if(disallowedLinks == null) {
				 return true;
			 }
			 for(String link : disallowedLinks) {
				 if(link.equals(filePath) || (filePath.startsWith(link) )) {
					 return false;
				 }
			 }	
			// currentCrawlDelay = robotInfo.getCrawlDelay("*");
		 }else if(robotInfo.containsUserAgent("*")) { //else we care about this directive
			 ArrayList<String> disallowedLinks = robotInfo.getDisallowedLinks("*");
			 if(disallowedLinks == null) {
				 return true;
			 }
			 for(String link : disallowedLinks) {
				 if(link.equals(filePath) || (filePath.startsWith(link)  )) {
					 return false;
				 }
			 }			
		 }
		 return true; //default
  }
  
  /*Creates a normalized URL of the form protocol // hostname:portNo/filepath. e.g: http://crawltest.cis.upenn.edu:80/foo.html
   * @param oldURL : The current url/page where we found the new link.
   * @param URLInfo: URLInfo object for current URL
   * @param redirectURL: New URL to visit, could be either relative or absolute
   *  */
  private String normalize(String oldURL, URLInfo urlInfo, String redirectURL) throws NullPointerException, IndexOutOfBoundsException {
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
  
  private String normalizeHttps(String oldURL, URL url, String redirectURL) throws NullPointerException, IndexOutOfBoundsException, MalformedURLException {
	  StringBuilder newURL;
	  if(redirectURL.startsWith("https")) { //already absolute
		   newURL = new StringBuilder(redirectURL);
		   //append port number
		   int port = url.getPort() == -1 ? HTTPS_PORT : url.getPort();
		   String portString = ":" + Integer.toString(port);
		   if(!redirectURL.contains(portString)){ //append port
			   int index = redirectURL.startsWith("https://") ? 8 : 7;
			   URL redirect = new URL(redirectURL);
			   if(url.getHost().equals(redirect.getHost())) {
				   index += url.getHost().length();
			   }else {
				   index += redirect.getHost().length();
			   }			   
			   newURL.insert(index, portString);
		   }		   		  	   
	  }else { //relative URL
		 
		   String temp = oldURL.substring(0, oldURL.lastIndexOf("/"));
		   newURL = new StringBuilder(temp);
		   newURL.append(redirectURL);
		  
	  } 
	  return newURL.toString();
  }
  
  
  /*Used to send an HTTP request
   * @param type : 0 is or sending HEAD requests and 1 is for sending GET requests
   * @param filePath: The path we are requesting ,e.g. /robots.txt or "/" for the home path
   * @param hostHeader: required for HTTP 1.1
   * @param lastModified: We send a lastModified request when we want to check if a document we've previously downloaded has changed since.  */
  private void sendRequest(int type, String filePath, String hostHeader, OutputStream out, Date lastModified) throws IOException {
	  /*Send HEAD/GET Request  */
	    String typeString = (type == 0) ? "HEAD " : "GET ";
		String headHeader = typeString + filePath + " HTTP/1.1\r\n";
		
		out.write(headHeader.getBytes());
	//	System.out.println(headHeader);
		out.write(hostHeader.getBytes());
		out.write(USER_AGENT_HEADER.getBytes());
		if(lastModified != null) {
			DateFormat df = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
			df.setTimeZone(TimeZone.getTimeZone("GMT"));
			String modifiedSince = "If-Modified-Since: " + df.format(lastModified) + "\r\n";
			out.write(modifiedSince.getBytes());
		}
		out.write(CLRF.getBytes());		
		out.flush();	
  }
  
  private void sendMonitoring(String url) throws IOException {
	  byte[] data = ("ransford;" + url).getBytes();
	  DatagramPacket packet = new DatagramPacket(data, data.length, hostMonitor, 10455);
	  s.send(packet);
  }
  
  /*Checks whether the document is a valid html or xml  */
  private boolean isValidType(String type) {
	  return (type.contains("text/html") || type.contains("text/xml") || type.endsWith(".html") || type.endsWith(".xml") || type.equals("application/xml"));
	 // return (type.contains("text/html") || type.endsWith(".html"));
  }
  
  private void parseRobotTxt(BufferedReader reader, String host) {
	  RobotsTxtInfo info = new RobotsTxtInfo();
	  String line;
		try {
			while((line = reader.readLine()) != null) {
				if(line.isBlank() || line.startsWith("#")) {
					continue;
				}
				String[] tuple = line.split(":");
				if(tuple.length != 2) {
					continue;
				}else {
					if (tuple[0].equals("User-agent")){								
						HashSet<String> agents = new HashSet<String>(); //set to hold all user agents for this directive
						agents.add(tuple[1].trim());
						info.addUserAgent(tuple[1].trim());
						String map = "";
						while((map = reader.readLine()) != null && !map.isBlank()) {
							String [] mapping = map.split(":");
							if(mapping.length != 2) {
								break;
							}
							if(mapping[0].equals("User-agent")) {
								agents.add(mapping[1].trim());
								info.addUserAgent(mapping[1].trim());
							}
							else if(mapping[0].equals("Disallow")) {
								for(String agent : agents) {
									info.addDisallowedLink(agent, mapping[1].trim());
								}										
							}else if(mapping[0].equals("Crawl-delay")) {
								for(String agent: agents) {
									info.addCrawlDelay(agent, Integer.parseInt(mapping[1].trim()));
								}									
							}else if(mapping[0].equals("Allow")) {
								for(String agent: agents) {
									info.addAllowedLink(agent, mapping[1].trim());
								}										
							}
						}
					}
				}
			}
		} catch (NumberFormatException | IOException e) {			
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}			
			DistributedCrawler.getInstance().getRobotMap().put(host, info);
		}
  }
  
    /**
     * Process a tuple received from the stream, tuple consists of a url which the crawler should crawl and outputs documents
     * 
     */
    @Override
    public void execute(Tuple input) {
    	 /*Basic Algorithm:
  	   *Intialize Q with a set of seed URLS
  	   *Pick the first URL from Q and download the corresponding page
  	   *Extract All URLS from the page
  	   *Append to Q any URLS that meet a) our criteria and b) are not already in P
  	   *Repeat whilst Q is not empty
  	   *  */
    	DistributedCrawler.getInstance().decrementInflightMessages(); //a message has been routed to this bolt hence we decrement number of inflight messages
    	if(DistributedCrawler.getInstance().getFileCount().get() >= DistributedCrawler.getInstance().getMaxFileNum()) {
    		DistributedCrawler.getInstance().shutdown();
    		System.out.println("Crawler Bolt Called Shutdown");
    		return;
    	}
        String url = input.getStringByField("url");
        if(url == null) { //: Check shutdown?
        	return;
        }
       // System.out.println("current queue size: " + DistributedCrawler.getInstance().getFrontier().getSize());
        DistributedCrawlerBolt.activeThreads.getAndIncrement(); //increment number of active threads
        HashMap<String, RobotsTxtInfo> robotMap = DistributedCrawler.getInstance().getRobotMap();
        HashMap<String, Date> lastCrawled = DistributedCrawler.getInstance().getLastCrawled();
        HttpsURLConnection conn = null;
        
        int c = DistributedCrawler.getInstance().getFileCount().get();
        if( c != 0 && c % 1000 == 0) {
        	System.out.println("file count: " + c);
        }
        
        if(url.startsWith("https://")) {
        	//System.out.println("received URL: " + url);
			  try {
					URL httpsUrl = new URL(url);					
					if(!robotMap.containsKey(httpsUrl.getHost())){ //get robots.txt
							
						String portString = ":" + Integer.toString(httpsUrl.getPort());
						int index;
						if(httpsUrl.getPort() != -1) {
							index = 8 + httpsUrl.getHost().length() + portString.length();	
						}else {
							index = 8 + httpsUrl.getHost().length();	
						}
						//System.out.println("url before: " + url);
						String robotUrl = url.substring(0, index) + "/robots.txt";
						URL robot = new URL(robotUrl);
						conn = (HttpsURLConnection) robot.openConnection();
						conn.setConnectTimeout(SOCKET_TIMEOUT);
						conn.setReadTimeout(READ_TIMEOUT);
						conn.setRequestMethod("GET");
						conn.setRequestProperty("User-Agent", USER_AGENT);
						//conn.get
						
						/*------------------------<*/
						sendMonitoring("/robots.txt");
						int responseCode = conn.getResponseCode();
						if(responseCode == HttpURLConnection.HTTP_OK) {						
							BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.US_ASCII));
							parseRobotTxt(reader, httpsUrl.getHost());						
						}					
						else {
							//System.out.println("robots not found");
							robotMap.put(robot.getHost(), new RobotsTxtInfo()); //no robots.txt found
						}
						conn.disconnect();
					}				
					/*-------------------->*/
					  String filePath = httpsUrl.getPath();
					  String hostName = httpsUrl.getHost();
						/*Check if we can crawl file path */
						RobotsTxtInfo robotInfo = robotMap.get(hostName);
						if(!canCrawl(robotInfo, filePath)) {
							System.out.println("cant crawl: " + url);
							DistributedCrawlerBolt.activeThreads.getAndDecrement();							
							return;  //: exit
						}
						/*Check crawl Delay  */
						if(robotInfo.crawlContainAgent(USER_AGENT) || (robotInfo.getCrawlDelay("*") != -1) ) {								
							if(lastCrawled.containsKey(hostName)) {
								int delay = robotInfo.crawlContainAgent(USER_AGENT) ? robotInfo.getCrawlDelay(USER_AGENT) : robotInfo.getCrawlDelay("*");
								//if(delay == -1) {
								//	delay = 1; //default crawl delay
								//}
								if(delay != -1) {
									Date previous = lastCrawled.get(hostName);
									Date now = new Date();
									if(now.getTime() - previous.getTime() < (delay * 1000)) { //checks if the last crawled time was longer than the crawl delay
										//DistributedCrawler.getInstance().getFrontier().enqueue(url); //re add back to queue and don't crawl
										//collector.emit(new Values<Object>(url, null, "false")); //emit document but tell next bolt not to store it
										//System.out.println("delay: " + url);
										//DistributedCrawler.getInstance().incrementInflightMessages();
										DistributedCrawler.getInstance().getFrontier().enqueue(url); //re add back to queue
										DistributedCrawlerBolt.activeThreads.getAndDecrement();
										return;
									}	
								}															
							}							
						}
							
						/*Send Head Request  */		
						DocVal doc = DistributedCrawler.getInstance().getDB().getDocInfo(url);
						conn = (HttpsURLConnection) httpsUrl.openConnection();
						conn.setRequestMethod("HEAD");
						conn.setConnectTimeout(SOCKET_TIMEOUT);
						conn.setReadTimeout(READ_TIMEOUT);
						conn.addRequestProperty("User-Agent", USER_AGENT);
						if (doc != null) {							
							conn.setIfModifiedSince(doc.getLastChecked().getTime());
						}	
						sendMonitoring(url);
						/*Check Head Response  */
						int responseCode = conn.getResponseCode();
						String type = conn.getContentType();
						//System.out.println("head type is: " + type + " url: " + url);
						int statusXX = responseCode / 100;
						DistributedCrawler.getInstance().getLinksCrawled().getAndIncrement(); //increment links crawled
						//System.out.println(" head response " + responseCode);
						if(responseCode == 304 && doc != null) { //don't download document, retrieve cached document and extract links
							//retrieve document
							//System.out.println(url + ": Not modified");	
							log.debug(url + ": Not modified");
							lastCrawled.put(hostName, new Date()); //update last crawled
							collector.emit(new Values<Object>(url, doc, "false")); //emit document but tell next bolt not to store it
							DistributedCrawler.getInstance().incrementInflightMessages();
							conn.disconnect();
							DistributedCrawlerBolt.activeThreads.getAndDecrement();
							return;
						}						
						if(statusXX == 3) { //check for redirects: 3xx URLS
							if(conn.getHeaderField("Location") != null) {
								String newUrl = normalizeHttps(url, httpsUrl, conn.getHeaderField("Location")); 					
							//	DistributedCrawler.getInstance().getFrontier().enqueue(newUrl);
								collector.emit(new Values<Object>(newUrl, null, "false")); //emit document but tell next bolt not to store it
								DistributedCrawler.getInstance().incrementInflightMessages();
							}else if(conn.getHeaderField("location") != null) {
								String newUrl = normalizeHttps(url, httpsUrl, conn.getHeaderField("Location")); 					
								//DistributedCrawler.getInstance().getFrontier().enqueue(newUrl);
								collector.emit(new Values<Object>(newUrl, null, "false")); //emit document but tell next bolt not to store it
								DistributedCrawler.getInstance().incrementInflightMessages();
							}
							conn.disconnect();
							DistributedCrawlerBolt.activeThreads.getAndDecrement();
							return; //continue run
						}
						if(statusXX == 4 || statusXX == 5) {
							conn.disconnect();
							DistributedCrawlerBolt.activeThreads.getAndDecrement();
							return;
						}																	
						if(type == null || !isValidType(type)) {
							conn.disconnect();
							DistributedCrawlerBolt.activeThreads.getAndDecrement();
							return;
						}	
											
						conn.disconnect();
						//conn.cl
						conn = (HttpsURLConnection) httpsUrl.openConnection();
						conn.setConnectTimeout(SOCKET_TIMEOUT);
						conn.setReadTimeout(READ_TIMEOUT);
						conn.setRequestMethod("GET");
						conn.addRequestProperty("User-Agent", USER_AGENT);
						//System.out.println("GET type is: " + conn.getContentType() + " url: " + url);
						sendMonitoring(url);
						lastCrawled.put(hostName, new Date()); //update last crawled
						responseCode = conn.getResponseCode();
						//System.out.println("get response " + responseCode);	
						if((responseCode / 100) != 2) {
							conn.disconnect();	
							DistributedCrawlerBolt.activeThreads.getAndDecrement();
							return;
						}
						long size = conn.getContentLengthLong();
						//System.out.println("size is: " + size);
						String body; 
						if( size > DistributedCrawler.getInstance().getmaxDocSize()) {
							conn.disconnect();
							DistributedCrawlerBolt.activeThreads.getAndDecrement();
							return;
						}						
						if(size == -1) { //size not specified so look for encoding
							String encoding = conn.getContentEncoding();
							if(encoding == null) {
								encoding = "UTF-8"; //default
							}
							body = IOUtils.toString(conn.getInputStream(), encoding);
						} else {
							body = parseHTTPSBody(size, conn.getInputStream());
						}
						 
						/*Emit document */
						String contentType = conn.getContentType();
						DocVal store = new DocVal( (int)size, contentType, body, new Date());
						//System.out.println(url + ": Downloading");
						log.debug(url + ": Downloading");
						//DistributedCrawler.getInstance().getFileCount().getAndIncrement();
						collector.emit(new Values<Object>(url, store, "true")); //emit document and tell next bolt to store doc	
						DistributedCrawler.getInstance().incrementInflightMessages();
						conn.disconnect();	
						DistributedCrawlerBolt.activeThreads.getAndDecrement();
					  /*<---------------------*/				
					
			} catch (MalformedURLException | DatabaseException | IndexOutOfBoundsException e) {
				//e.printStackTrace();
				log.debug(e.getMessage());
				if(conn != null) {
					conn.disconnect();
				}				
				DistributedCrawlerBolt.activeThreads.getAndDecrement();
				return;
			} catch (SocketTimeoutException | SSLException | SocketException e) {
				//e.printStackTrace();
				log.debug(e.getMessage());
				if(conn != null) {
					conn.disconnect();
				}				
				DistributedCrawlerBolt.activeThreads.getAndDecrement();
				return;	
			} catch (IOException | NullPointerException e) {
				//e.printStackTrace();
				log.debug(e.getMessage());
				if(conn != null) {
					conn.disconnect();
				}				
				DistributedCrawlerBolt.activeThreads.getAndDecrement();
				return;
			}
			  
		  } else {
			  //HTTP URLS
			  URLInfo urlInfo = new URLInfo(url);
			  if(urlInfo.getHostName() == null) { 
				  DistributedCrawlerBolt.activeThreads.getAndDecrement();
				  return; //skip this URL
			  }
			  		  
			  Socket socket = null;
			  try {
				  String portString = ":" + Integer.toString(urlInfo.getPortNo());
				  if(!url.contains(portString)) {
					  url = normalize(null, urlInfo, url);
				  }	
				//SocketAddress address = new InetSocketAddress(urlInfo.g)
				socket = new Socket();
				socket.connect(new InetSocketAddress(urlInfo.getHostName(), urlInfo.getPortNo()), SOCKET_TIMEOUT);
				//socket.tim
				String hostHeader = "Host: " + urlInfo.getHostName() + CLRF;
										
				OutputStream out = socket.getOutputStream();
				
				/*Get and parse robots.txt for this host */
				if(!robotMap.containsKey(urlInfo.getHostName())) {
					
					sendRequest(1, "/robots.txt",  hostHeader, out, null); 
					sendMonitoring("/robots.txt");
					HashMap<String, String> responseHeaders = new HashMap<String, String>();
					String body = parseResponse(responseHeaders, socket.getInputStream(), 1);
					int responseCode = Integer.parseInt(responseHeaders.get("Code"));
					
					if((responseCode / 100 == 2) && body != null) { //2xx response code only
						String robotsTxt = new String(body);				
						BufferedReader reader = new BufferedReader(new StringReader(robotsTxt));					
						parseRobotTxt(reader, urlInfo.getHostName());				
					}else {
						robotMap.put(urlInfo.getHostName(), new RobotsTxtInfo()); //no robots.txt found
					}
				}
				
				String filePath = urlInfo.getFilePath();
				/*Check if we can crawl file path */
				RobotsTxtInfo robotInfo = robotMap.get(urlInfo.getHostName());
				if(!canCrawl(robotInfo, filePath)) {
					socket.close();
					DistributedCrawlerBolt.activeThreads.getAndDecrement();
					return; //skip
				}
				String hostName = urlInfo.getHostName();
				/*Check crawl Delay  */
				if(robotInfo.crawlContainAgent(USER_AGENT) || (robotInfo.getCrawlDelay("*") != -1) ) {								
					if(lastCrawled.containsKey(hostName)) {
						int delay = robotInfo.crawlContainAgent(USER_AGENT) ? robotInfo.getCrawlDelay(USER_AGENT) : robotInfo.getCrawlDelay("*");
						Date previous = lastCrawled.get(hostName);
						Date now = new Date();
						if(now.getTime() - previous.getTime() < (delay * 1000)) { //checks if the last crawled time was longer than the crawl delay
							//DistributedCrawler.getInstance().getFrontier().enqueue(url); //re add back to queue and don't crawl
							collector.emit(new Values<Object>(url, null, "false")); //emit document but tell next bolt not to store it
							DistributedCrawler.getInstance().incrementInflightMessages();
							socket.close();
							DistributedCrawlerBolt.activeThreads.getAndDecrement();
							return;
						}
						lastCrawled.put(hostName, new Date());
					}			
				}
						
				/*Send Head Request  */		
				DocVal doc = StorageServer.getInstance().getDocInfo(url);
				
				if (doc != null) {				
					sendRequest(0, urlInfo.getFilePath(),  hostHeader, out, doc.getLastChecked()); //doc already exists, check if unmodified
				}else {
					sendRequest(0, urlInfo.getFilePath(),  hostHeader, out, null); 
				}	
				sendMonitoring(url);
				/*Check Head Response  */
				HashMap<String, String> responseHeaders = new HashMap<String, String>();
				String body = parseResponse(responseHeaders, socket.getInputStream(), 0);
				if(responseHeaders.isEmpty()) {
					socket.close();
					DistributedCrawlerBolt.activeThreads.getAndDecrement();
					return;
				}
				int responseCode = Integer.parseInt(responseHeaders.get("Code"));
				String type = responseHeaders.get("content-type");
				DistributedCrawler.getInstance().getLinksCrawled().getAndIncrement(); //increment links crawled
				
				if(responseCode == 304 && doc != null) { //don't download document, retrieve cached document and extract links
					//retrieve document
					//System.out.println(url + ": Not modified");	
					log.debug(url + ": Not modified");
					collector.emit(new Values<Object>(url, doc, "false")); //don't store
					DistributedCrawler.getInstance().incrementInflightMessages();
					lastCrawled.put(hostName, new Date()); //update last crawled
					socket.close();
					DistributedCrawlerBolt.activeThreads.getAndDecrement();
					return;
				}
				int statusXX = responseCode / 100;
				DistributedCrawler.getInstance().getLinksCrawled().getAndIncrement(); //increment links crawled
				if(statusXX == 3) { //check for redirects: 3xx URLS
					if(responseHeaders.get("location") != null) {
						String redirectURL = responseHeaders.get("location");
						String newUrl = null;
						if(redirectURL.startsWith("https://")) {
							URL httpsUrl = new URL(url);
							newUrl = normalizeHttps(url, httpsUrl, redirectURL); 
						}else {
							newUrl = normalize(url, urlInfo, redirectURL); 
						}					
						//DistributedCrawler.getInstance().getFrontier().enqueue(newUrl);
						collector.emit(new Values<Object>(newUrl, null, "false")); //emit document but tell next bolt not to store it
						DistributedCrawler.getInstance().incrementInflightMessages();
					}
					socket.close();
					DistributedCrawlerBolt.activeThreads.getAndDecrement();
					return; //continue run
				}
				if(statusXX == 4 || statusXX == 5) {
					socket.close();
					DistributedCrawlerBolt.activeThreads.getAndDecrement();
					return;
				}
				
				if(responseHeaders.get("content-length") == null){
					socket.close();
					DistributedCrawlerBolt.activeThreads.getAndDecrement();
					return;
				}
				
				if(type == null ||  !isValidType(type) ) {
					socket.close();
					DistributedCrawlerBolt.activeThreads.getAndDecrement();
					return;
				}
				
				int size = Integer.parseInt(responseHeaders.get("content-length"));
				if( size > DistributedCrawler.getInstance().getmaxDocSize()) {
					socket.close();
					DistributedCrawlerBolt.activeThreads.getAndDecrement();
					return;
				}
				
				sendRequest(1, urlInfo.getFilePath(),  hostHeader, out, null); //Get request
				sendMonitoring(url);
				lastCrawled.put(hostName, new Date()); //update last crawled
				
				responseHeaders = new HashMap<String, String>();
				body = parseResponse(responseHeaders, socket.getInputStream(), 1);
				responseCode = Integer.parseInt(responseHeaders.get("Code"));
				
				/*Emit file with flag to store in DB */
				String contentType = responseHeaders.get("content-type");
				DocVal store = new DocVal(size, contentType, body, new Date());
				//System.out.println(url + ": Downloading");
				log.debug(url + ": Downloading");
				collector.emit(new Values<Object>(url, store, "true"));
				//DistributedCrawler.getInstance().getFileCount().getAndIncrement();
				DistributedCrawler.getInstance().incrementInflightMessages();				
				socket.close();	
				DistributedCrawlerBolt.activeThreads.getAndDecrement();
			
			  }catch (UnknownHostException | NumberFormatException | MalformedURLException e) {
				  System.out.print("exception caught: ");
				  try {
					socket.close();
					DistributedCrawlerBolt.activeThreads.getAndDecrement();
					return;
				} catch (IOException e1) {
					e1.printStackTrace();
					DistributedCrawlerBolt.activeThreads.getAndDecrement();
					return;
				}
				  
			  }
			  catch (IOException | NullPointerException | DatabaseException | IndexOutOfBoundsException e) {
				  System.out.print("exception caught: ");
				//e.printStackTrace();
				try {
					if(socket == null) {
						DistributedCrawlerBolt.activeThreads.getAndDecrement();
						return;
					}
					socket.close();
					DistributedCrawlerBolt.activeThreads.getAndDecrement();
					return;
				} catch (IOException e1) {
					DistributedCrawlerBolt.activeThreads.getAndDecrement();
					return;
				}				
			}			  
		  }       
	  	}		 
      //  collector.emit(new Values<Object>(word, String.valueOf(count)));
    

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    	//System.out.println("WordCount executor " + getExecutorId() + " has words: " + wordCounter.keySet());
    	if(!s.isClosed()) { //close datagram socket
    		s.close();
    	}  	
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
