package edu.upenn.cis455.crawler.distributed;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import static spark.Spark.*;
import java.util.*;
import java.net.*;
import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.tuple.Fields;
import java.io.IOException;
import java.net.MalformedURLException;
import java.io.*;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.*;
import java.net.InetAddress.*;


public class MasterNode {

    // public static HashSet<String> workerAddress = new HashSet<String>();

    public static HashMap<String, WorkerStatus> workerInfo = new HashMap<String, WorkerStatus>();
    private static ObjectMapper mapper = new ObjectMapper();
    public static int myPort;
    public static String workerTable;
    
    // Data structure to hold the workers which have already started
    public static HashSet<String> startedWorkers = new HashSet<String>();
    private static int curWorkerIndex =0 ;

    private static int regWorkers = 0;


    /*
        Construct the worker table. The index is assigned in the order in which 
        the workers register with the worker
    */
    public static String constructWorkerTable(){
        

        HashMap<String, String> workerTable = new HashMap<String, String>(); 

        // int[] rand = new Random().ints(0, workerInfo.size()).distinct().limit(workerInfo.size()).toArray();
        int i =0 ;
        String output = null;
        for (String worker : workerInfo.keySet()){
            WorkerStatus value = workerInfo.get(worker);
            workerTable.put(String.valueOf(value.getIndex()), worker);
        }
        try{
            output = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(workerTable);
        }catch(Exception e){
            e.printStackTrace();
        }
        return output;
    }

    public static void sendWorkerTableToWorker(String workerAddress, String route) {
        try{
            if  ( Utils.sendJob(workerAddress, "POST",route,workerTable, "").getResponseCode() != 
                                HttpURLConnection.HTTP_OK) {
                            throw new RuntimeException("Job definition request failed");
                }
        }catch(Exception e ){
            e.printStackTrace();
        }
    }

    public static void sendRunJobToWorker(String worker) throws RuntimeException{

        //Send post request to start the job  
        try{  
            if(Utils.sendJob(worker,"POST", "startjob", "","").getResponseCode() != 
            HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Job definition request failed");
            } 
        }catch(Exception e ){
            e.printStackTrace();
        }
    }


    /*
        Construct html page for the status page 
    */
    public static String getStatus(){

         String template  = "<!DOCTYPE html><html><head><title>Crawler Status Page</title>"+
                        "<style>table, th, td {border: 1px solid black;border-collapse: collapse;}"+
                        "th, td {padding: 5px;text-align: left; }</style>";
        template+= "<h1>Status Page</h1><h3>Active workers : </h3></head>"+ "<body><p>{$BODY}</p></body></html>";


        String tableMessage = "<table style=\"width:100%\">"; 
            tableMessage+= "<tr><th>Worker Index </th>"+
                    "<th>Worker Address</th>"+
                    "<th>Status</th>"+
                    "<th>Links Crawled</th>" + 
                    "<th>Links Downloaded</th>" + 
                    "<th>Rate of Crawl (links/sec)</th>"+
                    "<th>ShutDown</th></tr>";

        for (Map.Entry mapElement : workerInfo.entrySet()){
            String worker = (String)mapElement.getKey();
            WorkerStatus info = (WorkerStatus)mapElement.getValue();
            tableMessage+= "<tr><td>"+ info.getIndex()+"</td>"+
                            "<td>"+worker+"</td>"+
                            "<td>"+info.getStatus()+"</td>"+
                            "<td>"+info.getCrawlLinks()+"</td>"+
                            "<td>"+info.getCrawlDownloaded()+"</td>"+
                            "<td>"+info.getCrawlRate()+"</td>"+
                            "<td> <form action=\"/shutdown?worker="+worker+"\" method=\"POST\">"+
                            "<input type=\"submit\" value=\"Shutdown\" /></form></td></tr>";
        }

        tableMessage+="</table>";
        String message = template.replace("{$BODY}", tableMessage);
        return message;
    }

    



    public static void main(String args[]){

        

        if(args.length ==0){
            System.out.println("Invalid format. Pass the number of workers. ");
            System.exit(1);
        }

        myPort = Integer.parseInt(args[0]);

        try{

            InetAddress inetAddress = InetAddress.getLocalHost();
            System.out.println("Host Addrees " + inetAddress.getHostAddress());
            System.out.println("Creating server listener at socket " + myPort);
           // setPort(myPort);
            port(myPort);
        }catch(Exception e){
            e.printStackTrace();
            System.exit(1);
        }

        int numWorkers = Integer.parseInt(args[1]);
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        
        /*
            WORK IN PROGRESS : For dynamic addition of worker

        */

        // get("/registerWorker", (request, response)->{
        //     try{

        //         String workerIp = request.ip();
        //         String workerPort = String.valueOf(request.queryParams("port"));
        //         String workerEntry = workerIp+":"+workerPort;


        //         String status = request.queryParams("status");

        //         if(!workerInfo.containsKey(workerEntry)){
                    
        //             Date now = new Date();

        //             WorkerStatus newEntry = new WorkerStatus(curWorkerIndex++);
        //             newEntry.setStatus(status);
        //             newEntry.setLastSeen(now);

        //             workerInfo.put(workerEntry, newEntry);
        //             regWorkers++;
        //         }

        //         if ((startedWorkers.size() == 0 && regWorkers == numWorkers) || (startedWorkers.size() >0)){
        //                 workerTable = constructWorkerTable();
        //         }

        //         /*
        //             All workers registered. go ahead, start the job
        //         */

        //         if (regWorkers == numWorkers){
        //             for(String worker : workerInfo.keySet()){
        //                 if(!startedWorkers.contains(worker)){
                            
        //                     WorkerStatus value = workerInfo.get(worker);
        //                     String workerRoute = "workertable?index="+value.getIndex(); 
        //                     System.out.println((worker));

        //                     //Send post request with the table 
        //                     sendWorkerTableToWorker(worker,workerRoute);

        //                     //Send post request to start the job    
        //                     sendRunJobToWorker(worker); 
        //                 }
        //             }
        //         }


        //         /*
        //             New Worker added. Send them an updated table and run job to the new worker
        //         */
        //         if(startedWorkers.size()>0){

        //             for(String worker : workerInfo.keySet()){
        //                 WorkerStatus value = workerInfo.get(worker);
        //                 String workerRoute = "workertable?index="+value.getIndex(); 
        //                 System.out.println((worker));

        //                 //Send post request with the table 
        //                 sendWorkerTableToWorker(worker, workerRoute);

        //                 if(!startedWorkers.contains(worker)){
        //                     sendRunJobToWorker(worker); 
        //                 }
        //             }

                    
        //         }


        //     }catch(Exception e){
        //         e.printStackTrace();
        //     }

        //     return "Registered Worker";

        // });




        /*
            Status message sent from the worker which can be used for 
            keeping tabs on the worker
        */

        get("/workerstatus", (request, resposne) ->{
            try{
                System.out.println("recieved worker ");
                String workerIp = request.ip();
                String workerPort = String.valueOf(request.queryParams("port"));
                String workerEntry = workerIp+":"+workerPort;


                String status = request.queryParams("status");
                int linksCrawled = Integer.parseInt(request.queryParams("crawled"));
                int linksDownloaded = Integer.parseInt(request.queryParams("downloaded"));
                double rate = Double.parseDouble(request.queryParams("rate")); 
                if(!workerInfo.containsKey(workerEntry)){
                    
                    Date now = new Date();

                    WorkerStatus newEntry = new WorkerStatus(curWorkerIndex++);
                    newEntry.setStatus(status);
                    newEntry.setLastSeen(now);

                    workerInfo.put(workerEntry, newEntry);
                    regWorkers++;
                }else{
                	if(status.equals("idle")) { 
                		regWorkers++; //restart node
                	}else {
                		workerInfo.get(workerEntry).setLastSeen(new Date());
                        workerInfo.get(workerEntry).setStatus(status);
                        workerInfo.get(workerEntry).setCrawlLinks(linksCrawled);
                        workerInfo.get(workerEntry).setCrawlDownloaded(linksDownloaded);
                        workerInfo.get(workerEntry).setCrawlRate(rate);
                	}                   
                }

                if(regWorkers == numWorkers){
                    workerTable = constructWorkerTable();
                }
                    
                if(regWorkers >= numWorkers){
                    
                    for(String worker : workerInfo.keySet()){
                        /* Unless the worker has sent an acknowledgment that it has recieved the worker table,
                        keep sending the workerTable and the command to start job
                        */
                        if(!startedWorkers.contains(worker)){
                            
                            WorkerStatus value = workerInfo.get(worker);
                            String workerRoute = "workertable?index="+value.getIndex(); 
                            System.out.println((worker));

                            //Send post request with the table 
                            sendWorkerTableToWorker(worker , workerRoute);

                            //Send post request to start the job    
                            sendRunJobToWorker(worker); 
                        }
                    }
                }
                  
            }catch(Exception e ){
                // e.printStackTrace();
            }

            return "OK";
        });

        //Post request to track the workers who have recieved the startJob request from the master
        post("/workerstarted", (request, response)-> {
            
            try{
                System.out.println("Recieved worker start Acknowledgment");
                String workerEntry = request.ip()+":"+ String.valueOf(request.queryParams("port"));
                startedWorkers.add(workerEntry);
                System.out.println(workerEntry);

            }catch(Exception e){
                e.printStackTrace();
            }

            return "OK";

        });

        post("/shutdown" ,(request, response)->{
            try{
                String workerToKill = request.queryParams("worker");

                workerInfo.remove(workerToKill);
                workerTable = constructWorkerTable();
                regWorkers--;

                for(String worker : workerInfo.keySet()){
                    /* 
                        Send the updated table to every worker
                    */
                    WorkerStatus value = workerInfo.get(worker);
                    String workerRoute = "workertable?index="+value.getIndex();    
                    System.out.println((worker));
                    sendWorkerTableToWorker(worker, workerRoute);

                }

                if  ( Utils.sendJob(workerToKill, "POST","shutdown","", "").getResponseCode() != 
                                    HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Shutdown worker request failed");
                }
            }catch(Exception e){
                // e.printStackTrace();
            }

            return "Shutting down Worker";
        });

        get("/", (request,response)-> {
            String message = getStatus();
            return message;
        });
    }
}