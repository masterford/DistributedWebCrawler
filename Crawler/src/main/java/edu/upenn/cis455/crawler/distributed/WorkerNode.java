package edu.upenn.cis455.crawler.distributed;


import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.tuple.Fields;
import static spark.Spark.setPort;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

// import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyContext;
// import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis455.crawler.URLFrontier;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;
import java.util.*;
import static spark.Spark.*;
import java.io.*;
import java.net.*;
import java.net.InetAddress.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class WorkerNode {

    private static HashMap<String, String> workerTable = new HashMap<String, String>();
    private static int myPortNum = 0;
    private static String masterUrl;
    private static URLFrontier frontier = DistributedCrawler.getInstance().getFrontier();
    static private LocalCluster cluster = new LocalCluster();
    private static String status = "idle";
    private static Topology topo ;
    private static boolean shutDownCalled = false;
    public static int workerIndex = 0;
    public static HashSet<String> receivedURLs = new HashSet<String>(); //URLS received from host splitter
    private static String currentWorkerAddress;
    private static boolean hasStarted = false;

    static WorkerMonitor monitor = new WorkerMonitor();

    // private static final WorkerNode instance = new WorkerNode();

    // public static WorkerNode getInstance(){
    //     return instance;
    // }

    public static HashMap<String, String> getWorkerTable(){
        return workerTable;
    }

    public static int getPort(){
        return myPortNum;
    }

    public static String getMasterUrl(){
        return masterUrl;
    }
    
    public static String getStatus(){
        return status;
    }

    public void startMonitor(){
        monitor.start();
    }
    public static boolean isShutdown(){
        return shutDownCalled;
    }

    public static void setShutDown(){
        shutDownCalled = true;
    }
    
    public static int getLinksCrawled() {
    	if(!hasStarted) {
    		return 0;
    	}else {
    		return DistributedCrawler.getInstance().getLinksCrawled().get();
    	}
    	
    }
    
    public static int getLinksDownloaded() {
    	if(!hasStarted) {
    		return 0;
    	}else {
    		return DistributedCrawler.getInstance().getFileCount().get();
    	}   	
    }
    
    public static String getWorkerAddress(){
        return currentWorkerAddress;
    }

    public WorkerNode() {

        port(myPortNum);

        startMonitor();
        
        try{
            InetAddress inetAddress = InetAddress.getLocalHost();
            System.out.println("Host Addrees " + inetAddress.getHostAddress());
            currentWorkerAddress = inetAddress.getHostAddress()+":"+String.valueOf(myPortNum);
        }catch(Exception e ){
            e.printStackTrace();
        }

        //Intialize the worker to route all the URLs to itself 
        workerTable.put("0", currentWorkerAddress);

        System.out.println("Current worker Address "+ currentWorkerAddress);

        final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        Spark.post("/workertable", new Route() {

            @Override
            public Object handle(Request arg0, Response arg1) {
                System.out.println("recieved worker Table");
                workerIndex = Integer.parseInt(arg0.queryParams("index"));
                System.out.println("workerindex " + workerIndex);
                try{
                    synchronized(workerTable){
                        workerTable = om.readValue(arg0.body(), HashMap.class);
                    }
                    System.out.println(workerTable);
                    return "worker Table Recieved";

                }catch(Exception e ){
                    e.printStackTrace();
                }

                return "OK";
            }
        });


        Spark.post("/startjob", new Route() {

            @Override
            public Object handle(Request arg0, Response arg1) {
                try{

                    //TO Start Cluster
                    System.out.println("Recieved a start job"); 
                    status = "Crawling";
                    Config config = new Config();
                    topo = getTopology();
                    if(topo != null ){
                        System.out.println("Starting job");   
                        cluster.submitTopology("LocalCluster", config,  topo);
                        hasStarted = true;
                    }

                    if(Utils.sendJob(masterUrl,"POST", "workerstarted", "port="+myPortNum, 
                                    "application/x-www-form-urlencoded").getResponseCode() != 
                                    HttpURLConnection.HTTP_OK) {
                        throw new RuntimeException("Job definition request failed");
                    }

                }catch(Exception e){
                    e.printStackTrace();
                }

                return "OK";
            }
        });

        Spark.post("/urlroute", new Route(){

            public Object handle(Request arg0, Response arg1) {
                try{
                    String url = (String)arg0.body();
                    //System.out.println("url Recieved "+ url);

                    //Back to frontier
                   // DistributedCrawler.getInstance().getFrontier().enqueue(url);
                    synchronized(receivedURLs) {
                    	receivedURLs.add(url);
                    }                   
                }catch(Exception e ){   
                    e.printStackTrace();
                }
                return "OK";
            }
        });

        Spark.post("/shutdown", new Route(){

            public Object handle(Request arg0, Response arg1) {
               
            	shutDown();
                return "shutting down";
            }

        });



        Spark.get("/hello", new Route(){
            public Object handle(Request arg0, Response arg1) {
                return "OK";
            }
        });
            

    }
    
    public static void setStatus(String newStatus) {
    	status = newStatus;
    }
    
    public static void shutDown() {
    	 try{
             setShutDown();
             if(!DistributedCrawler.getInstance().getShutdown()) {
            	 DistributedCrawler.getInstance().shutdown();
             }
             monitor.interrupt();
             cluster.killTopology("LocalCluster");
             cluster.shutdown();
             System.exit(0);
         }catch(Exception e){
             e.printStackTrace();
         }
    }

    /*
        Note for Ransford: This is the function to change. The first parts reads the json files from
                            the json file and then you can build your cluster and submit them..
    */
    public static Topology getTopology(){
        JSONParser parser = new JSONParser();
        
        try{
            String file = System.getProperty("user.dir")+"/CrawlerConfigs/worker_"+String.valueOf(workerIndex)+".json";
            Object obj = parser.parse(new FileReader(file));

            JSONObject jsonObj = (JSONObject) obj;
            
            JSONArray seed = (JSONArray) jsonObj.get("seed");
            JSONArray args = (JSONArray) jsonObj.get("args");
            
            Iterator<String> iterator = seed.iterator();

            /*populate frontier  */
            while(iterator.hasNext()){
            	String url = iterator.next();
            	System.out.println("seed: " + url);
                if(frontier == null){
                	DistributedCrawler.getInstance().initFrontier(url);
                    //frontier = new URLFrontier(iterator.next());
                }else{
                	DistributedCrawler.getInstance().getFrontier().enqueue(url);
                   // frontier.enqueue(iterator.next());
                }
            }
            
            /*Setup Crawler  */
            int i = 0;
            String[] argsString = new String[args.size()];
           for(Object ob : args.toArray()) {
        	   argsString[i] = ob.toString();
        	   i++;
           }
            DistributedCrawler.getInstance().setup(argsString);
            
            /*
            testUrlSpout spout = new testUrlSpout();
            HostSplitterBolt hostSplitter = new HostSplitterBolt();

            TopologyBuilder builder = new TopologyBuilder();

            // Only one source ("spout") for the words
            builder.setSpout("WORD_SPOUT", spout, 1);
            
            // Parallel mappers, each of which gets specific words
            builder.setBolt("Host_BOLT", hostSplitter, 1).shuffleGrouping("WORD_SPOUT"); */           
            Topology topo = DistributedCrawler.getInstance().createTopology();
            return topo;

        }catch(Exception e ){

            e.printStackTrace();
        }

        return null;        
        
    }

    public static void main(String args[]){
        if(args.length == 0){
            System.out.println("Error in the number of arguments. Arguments to be supplied are (portNumber, masterUrl)");
            System.exit(1);
        }

        myPortNum = Integer.parseInt(args[0]);
        // port(myPortNum);
        masterUrl = args[1];
        new WorkerNode();

    }

}