package edu.upenn.cis455.crawler.distributed;

import java.util.*;
public class WorkerStatus {

    String status;
    Date lastSeenTime;
    Date started;
    Date finished;
    double duration = 0.0;
    String currentCrawl;
    int linksCrawled = 0;
    int linksDownloaded = 0;
    double maxCrawlRate =0;
    double avgCrawlRate =0;
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
    public void setStarted(Date time){
        started = time;
    }

    public Date getStarted(){
        return started;
    }
    
    public void setFinished(Date time){
        started = time;
    }

    public Date getFinished(){
        return finished;
    }

    public int getCrawlLinks(){
        return linksCrawled;
    }
    public void setCrawlLinks(int links){
        linksCrawled = links;
    }
    
    public void setDuration(Date now) {
    	duration = (now.getTime() - started.getTime()) / (1000 * 60);
    }
    
    public double getDuration() {
    	return duration;
    }
    
    public int getCrawlDownloaded(){
        return linksDownloaded;
    }
    
    public void setCrawlDownloaded(int sum){
        linksDownloaded = sum;
    }

    public double getMaxCrawlRate(){
        return maxCrawlRate;
    }
    
    public void setMaxCrawlRate(double rate){
         maxCrawlRate = rate;
    }
    
    public double getAvgCrawlRate(){
        return avgCrawlRate;
    }
    
    public void setAvgCrawlRate(double rate){
         avgCrawlRate = rate;
    }

}