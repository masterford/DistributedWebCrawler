package edu.upenn.cis455.crawler.distributed;


import java.lang.*;
import java.net.MalformedURLException;

import edu.upenn.cis455.crawler.distributed.WorkerNode;

import java.net.*;
import java.io.*;
public class WorkerMonitor  extends Thread{
	private int previous = 0;
    

    public WorkerMonitor(){

    }
    //TODO: Change the way you are setting the keys read and written
    public String constructGetRequest(){
    	int current = WorkerNode.getLinksCrawled();
    	double rate = 0.0;
    	rate = ( (double) current - (double) previous) / 10;
    	//if(WorkerNode.hasStarted) {
    	//	//System.out.println("current is : " + current + " previous is: " + previous + " calculated rate is: " + rate);
    	//}  	
    	previous = current;
       String url =  "http://"+ WorkerNode.getMasterUrl()+"/workerstatus?port=" + WorkerNode.getPort()
                    + "&status="+WorkerNode.getStatus()+ "&crawled="+WorkerNode.getLinksCrawled() + "&downloaded="+WorkerNode.getLinksDownloaded() + "&rate="+rate;
        
        return url;
    }

    public void run(){
    	HttpURLConnection conn = null;
        while(!WorkerNode.isShutdown()){
        	//HttpURLConnection conn;
            try{

                // System.out.println("Getting url "+constructGetRequest());
                
                URL url = new URL(constructGetRequest());
                
                conn = (HttpURLConnection)url.openConnection();
                conn.setDoOutput(true);
                conn.setRequestMethod("GET");
                conn.getResponseCode();
               // System.out.println("Sendin status " + conn.getResponseCode());

                //Sleep for 10 seconds
                conn.disconnect();
                Thread.sleep(10000);

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