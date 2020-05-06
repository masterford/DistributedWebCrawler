package edu.upenn.cis455.crawler;

import java.util.LinkedList;
import java.util.Queue;

public class URLFrontier {

	private Queue<String> frontier;
	
	public URLFrontier(String seed) {
		frontier = new LinkedList<String>();
		frontier.add(seed);
	}
	
	public synchronized String dequeue() {
		return frontier.poll();
	}
	
	public synchronized void enqueue(String url) {
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
