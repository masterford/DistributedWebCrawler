package edu.upenn.cis455.crawler.distributed;

import edu.upenn.cis455.crawler.distributed.*;
import java.io.*;
import java.net.MalformedURLException;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.tools.sjavac.Log;

public class HostSplitterBolt  implements IRichBolt{

    Fields myFields = new Fields("url");

    String executorId = UUID.randomUUID().toString();

    OutputCollector collector;
    
    private static AtomicInteger activeThreads;

    public static int getActiveThreads() {
    	return HostSplitterBolt.activeThreads.get();
    }
    
    public static HttpURLConnection sendJob(String dest, String reqType,String job, String parameters) throws IOException {
    
        if(!dest.contains("http://")){
          dest = "http://"+dest;
        }
        
        URL url = new URL(dest + "/" + job);
        
      // System.out.println("Sending request to " + url.toString());
        
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod(reqType);
        
        if (reqType.equals("POST")) {
            conn.setRequestProperty("Content-Type", "application/json");
            
            OutputStream os = conn.getOutputStream();
            byte[] toSend = parameters.getBytes();
            os.write(toSend);
            os.flush();
    
            // System.out.println("Sent data");
        } else
            conn.getOutputStream();
        
        return conn;
      }

    public HostSplitterBolt(){
    	activeThreads = new AtomicInteger();
    }

    @Override
	public void cleanup() {
		// Do nothing

    }


    @Override
	public void execute(Tuple input) {
        // 
    	DistributedCrawler.getInstance().decrementInflightMessages();
    	
    	HostSplitterBolt.activeThreads.getAndIncrement();
        String host = input.getStringByField("host");
        String url = input.getStringByField("url");
        // System.out.println("Got host "+ host + " url "+ url);
       try{
    	   if(WorkerNode.receivedURLs.size() >= 100 || DistributedCrawler.getInstance().getFrontier().isEmpty()) {
    		   synchronized(WorkerNode.receivedURLs) {
        		   for(String receivedUrl : WorkerNode.receivedURLs) {
            		   this.collector.emit(new Values<Object>(receivedUrl)); //emit to URLFilter
            		   DistributedCrawler.getInstance().incrementInflightMessages();  //signals a message is currently being routed
            	   }
        		   WorkerNode.receivedURLs.clear();
        	   }  
    	   }
    	   	   
            synchronized(WorkerNode.getWorkerTable()){
                int hostNum = Math.abs(host.hashCode() % (WorkerNode.getWorkerTable().size()));
                if(hostNum == WorkerNode.workerIndex) {               	
                     this.collector.emit(new Values<Object>(url));
                     DistributedCrawler.getInstance().incrementInflightMessages();  //signals a message is currently being routed
                }else {
                	Set<String> keys = WorkerNode.getWorkerTable().keySet();
                    String[] array = keys.toArray(new String[keys.size()]);
                   // System.out.println(WorkerNode.getWorkerTable());
                   // System.out.println("Got host number "+ hostNum+" for URL "+ host);        
                    String address = WorkerNode.getWorkerTable().get(String.valueOf(array[hostNum]));
                   // System.out.println("address: " + address);
                   // System.out.println("worker address: " + WorkerNode.getWorkerAddress());
                   // if(address.equals(WorkerNode.getWorkerAddress())){
                       // System.out.println("Forwarding "+ url + " to filter");
                       // DistributedCrawler.getInstance().getFrontier().enqueue(url);
                       // this.collector.emit(new Values<Object>(url));
                       // DistributedCrawler.getInstance().incrementInflightMessages();  //signals a message is currently being routed
                  //  }else{
                    if  ( sendJob(address, "POST","urlroute",url).getResponseCode() != 
                            HttpURLConnection.HTTP_OK) {
                        //throw new RuntimeException("Job definition request failed");
                    	Log.debug("HostSpliter bolt couldn't forward URL");
                    	this.collector.emit(new Values<Object>(url));
                        DistributedCrawler.getInstance().incrementInflightMessages();
                    }
                }               
            }
        }catch(IOException e) {
        	Log.debug("HostSpliter bolt couldn't forward URL");
        	this.collector.emit(new Values<Object>(url));
            DistributedCrawler.getInstance().incrementInflightMessages();
        } catch(Exception e){
            e.printStackTrace();
        } finally {
        	HostSplitterBolt.activeThreads.decrementAndGet();
        }
    }


    @Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
        // Do nothing
         this.collector = collector;
	}

	@Override
	public String getExecutorId() {
		return executorId;
    }
    
    @Override
	public void setRouter(IStreamRouter router) {
         this.collector.setRouter(router);
    }
    

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}