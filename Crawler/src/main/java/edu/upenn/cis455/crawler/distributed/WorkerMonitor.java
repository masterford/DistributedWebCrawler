package edu.upenn.cis455.crawler.distributed;

import java.net.MalformedURLException;

import edu.upenn.cis455.crawler.distributed.WorkerNode;

import java.net.*;

/*UThis class is used by the worker node to send status updates to the master every 10 seconds  */
public class WorkerMonitor  extends Thread{
	private int previous = 0;
    private double max = 0.0;
    private double avgRate = 0.0;
    private long duration = 0;

    public WorkerMonitor(){

    }
    
    public String constructGetRequest(){
    	int current = WorkerNode.getLinksCrawled();
    	double rate = 0.0;
    	rate = ( (double) current - (double) previous) / 10;
    	if(rate > max) {
    		max = rate;
    	}
    	if(duration != 0 && !WorkerNode.crawlerFinished()) {
    		avgRate = WorkerNode.getLinksDownloaded() / duration; //total links downloaded/total seconds elapsed
    	}    	 	
    	previous = current;
    	
    	int http2xx = WorkerNode.getHttp200();
    	int http3xx = WorkerNode.getHttp3xx();
    	int httpOther = WorkerNode.getHttpOther();
    	int http404 = WorkerNode.getHttp404();
    	
       String url =  "http://"+ WorkerNode.getMasterUrl()+"/workerstatus?port=" + WorkerNode.getPort()
                    + "&status="+WorkerNode.getStatus()+ "&crawled="+WorkerNode.getLinksCrawled() + "&downloaded="+WorkerNode.getLinksDownloaded()
                    + "&max="+max + "&avg="+avgRate + "&http2xx="+http2xx + "&http3xx="+http3xx + "&other="+httpOther + "&http404="+http404;
        
        return url;
    }

    public void run(){
    	HttpURLConnection conn = null;
        while(!WorkerNode.isShutdown()){
            try{

                 //System.out.println("Getting url "+constructGetRequest());
                
                URL url = new URL(constructGetRequest());
                
                conn = (HttpURLConnection)url.openConnection();
                conn.setDoOutput(true);
                conn.setRequestMethod("GET");
                conn.getResponseCode();               

                //Sleep for 10 seconds
                conn.disconnect();
                Thread.sleep(10000);
                if(!WorkerNode.crawlerFinished()) {
                	duration += 10;
                }               

            }catch(MalformedURLException e ){
                e.printStackTrace();
                continue;
            }catch(Exception e){
                //Do Nothing. Basically keep trying till killed
            	if(conn != null) {
            		conn.disconnect();
            	}
            	continue;
            	
            } finally {
            	if(conn != null) {
            		conn.disconnect();
            	}
            }
        }


        
    }
}