package edu.upenn.cis455.crawler.distributed;

import java.util.*;
public class WorkerStatus {

    String status;
    Date lastSeenTime;
    String currentCrawl;
    int linksCrawled = 0;
    int linksDownloaded = 0;
    double crawlRate =0;
    int workerIndex=0;

    public WorkerStatus(int index){
        workerIndex = index;
    }

    public int getIndex(){
        return workerIndex;
    }

    public void setStatus(String st){
        status = st;
    }

    public String getStatus(){
        return status;
    }

    public void setLastSeen(Date time){
        lastSeenTime = time;
    }

    public Date getLastSeen(){
        return lastSeenTime;
    }

    public int getCrawlLinks(){
        return linksCrawled;
    }
    public void setCrawlLinks(int links){
        linksCrawled = links;
    }
    
    public int getCrawlDownloaded(){
        return linksDownloaded;
    }
    
    public void setCrawlDownloaded(int sum){
        linksDownloaded = sum;
    }

    public double getCrawlRate(){
        return crawlRate;
    }

    public void setCrawlRate(double rate){
        crawlRate = rate;
    }



}