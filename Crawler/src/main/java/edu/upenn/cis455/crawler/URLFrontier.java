/*
 * @author:Ransford Antwi
 */

package edu.upenn.cis455.crawler;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

public class URLFrontier {

	private Queue<String> frontier;
	private HashMap<String, Integer> consecutiveCount; //map of hostname to number of consecutive urls from this hostName
	private HashSet<String> priorityOne; //priority one URLS get 15 consecutive URLs
	private HashSet<String> priorityTwo; //priority two URLs get 10 consecutive URLS
	final int PRIORITY_ONE_COUNT = 15;
	final int PRIORITY_TWO_COUNT = 10;
	final int DEFAULT_COUNT = 5;
	
	public URLFrontier(String seed) {
		frontier = new LinkedList<String>();
		consecutiveCount = new HashMap<String, Integer>(); //will only contain one host at a time
		priorityOne = new HashSet<String>();
		priorityTwo = new HashSet<String>();
		frontier.add(seed);  
		
		/*populate priorities */
		priorityOne.add("en.wikipedia.org");
		priorityOne.add("upenn.edu");
		priorityOne.add("cnn.com");
		
		priorityTwo.add("stackoverflow.com");
		priorityTwo.add("reddit.com");
		priorityTwo.add("bbc.com");
	}
	
	public void addPriorityOne(String host) {
		priorityOne.add(host);
	}
	
	public void removePriorityOne(String host) {
		priorityOne.remove(host);
	} 
	
	public void addPriorityTwo(String host) {
		priorityTwo.add(host);
	}
	
	public void removePriorityTwo(String host) {
		priorityTwo.remove(host);
	} 
	 
	/*Removes the next item from the frontier whilst taking into account priorities */
	public synchronized String dequeue() {
		URL urlObject;
		try {
			String url = frontier.peek();
			urlObject = new URL(url);
			String host = urlObject.getHost();
			int count = 0;
			if(consecutiveCount.containsKey(host)) {
				count = consecutiveCount.get(host) + 1;
				consecutiveCount.put(host, count);
			}else {
				consecutiveCount.clear(); //new host, restart consecutive count
				consecutiveCount.put(host,  1);
				count = 1;
			}
			
			if(priorityOne.contains(host)) {
				if(count <= PRIORITY_ONE_COUNT) {
					return frontier.poll();
				}else {
					int currentSize = frontier.size();
					int loop = 0;
					while(loop < currentSize) { //keep iterating until we find a different host name
						String next = frontier.poll();
						URL nextUrl = new URL(next);
						String nextHost = nextUrl.getHost();
						if(!host.equals(nextHost)) {
							return next;
						}else {
							frontier.add(next); //re add back to the end of the queue
							loop++;
						}
					}					
					return frontier.poll(); //at this stage just return what we have
				}
				
			} else if(priorityTwo.contains(host)) {
				if(count <= PRIORITY_TWO_COUNT) {
					return frontier.poll();
				}else {
					int currentSize = frontier.size();
					int loop = 0;
					while(loop < currentSize) { //keep iterating until we find a different host name
						String next = frontier.poll();
						URL nextUrl = new URL(next);
						String nextHost = nextUrl.getHost();
						if(!host.equals(nextHost)) {
							return next;
						}else {
							frontier.add(next); //re add back to the end of the queue
							loop++;
						}
					}					
					return frontier.poll(); //at this stage just return what we have
				}
			} else  {
				if(count <= DEFAULT_COUNT) {
					return frontier.poll();
				}else {
					int currentSize = frontier.size();
					int loop = 0;
					while(loop < currentSize) { //keep iterating until we find a different host name
						String next = frontier.poll();
						URL nextUrl = new URL(next);
						String nextHost = nextUrl.getHost();
						if(!host.equals(nextHost)) {
							return next;
						}else {
							frontier.add(next); //re add back to the end of the queue
							loop++;
						}
					}					
					return frontier.poll(); //at this stage just return what we have
				}
			}
		} catch (MalformedURLException e) {
			
		}
		return frontier.poll();
	}
	
	public void enqueue(String url) {				
		frontier.add(url);
	}
	
	public boolean isEmpty() {
		return frontier.isEmpty();
	}
	
	public void clear() {
		frontier.clear();
		return;
	}
	
	public synchronized int getSize() {
		return frontier.size();
	}
}
