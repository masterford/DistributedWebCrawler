package edu.upenn.cis455.crawler.distributed;

import java.util.*;
import java.net.*;
import java.io.*;

public class Utils {



    public static HttpURLConnection sendJob(String dest, String reqType,String job, String parameters, String format) throws IOException {
    
        if(!dest.contains("http://")){
          dest = "http://"+dest;
        }

        
        
        URL url = new URL(dest + "/" + job);
        
       System.out.println("Sending request to " + url.toString());
        
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod(reqType);
        try{
            if (reqType.equals("POST")) {
                if(format.equals(""))
                    conn.setRequestProperty("Content-Type", "application/json");
                else
                    conn.setRequestProperty("Content-Type", format);
                
                OutputStream os = conn.getOutputStream();
                byte[] toSend = parameters.getBytes();
                os.write(toSend);
                os.flush();
        
                // System.out.println("Sent data");
            } else
                conn.getOutputStream();


        }catch(Exception e ){
            // if(!job.contains("shutdown"))
            //     e.printStackTrace();
            //
        }
        
        return conn;
      }
}