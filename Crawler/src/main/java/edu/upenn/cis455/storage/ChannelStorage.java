package edu.upenn.cis455.storage;

import java.io.Serializable;
import java.util.ArrayList;

/*Class to store information about documents  */
public class ChannelStorage implements Serializable {
	private String channelName;	
	private String owner;
	private String XPath;
	private static final long serialVersionUID = 7526571155622776145L;
	private ArrayList<String> documentURLs;  //URL of document that matched an XPath in the channel
	private ArrayList<String> subscribedUsers;
	
	public ChannelStorage(String channelName, String XPath, String ownerUserName) {
		this.channelName = channelName;
		this.owner = ownerUserName;
		this.XPath = XPath;
		documentURLs = new ArrayList<String>();
		subscribedUsers = new ArrayList<String>();
	}
	
	public String getName() {
		return this.channelName;
	}
	
	public String getOwner() {
		return this.owner;
	}
	
	public String getXPath() {
		return this.XPath;
	}
	public ArrayList<String> getDocumentURLs() { //returns the list of all urls for documents that matched on this channel
		return this.documentURLs;
	}
	
	public ArrayList<String> getSubscribedUsers() { //returns the list of all urls for documents that matched on this channel
		return this.subscribedUsers;
	}
	
	public void addDocumentURLs(String url) { //Adds another url corresponding with a matched document
	   this.documentURLs.add(url);
	}
	
	public void addSubscribedUsers(String user) { //Adds another url corresponding with a matched document
		this.subscribedUsers.add(user);
	}
	
	public void removeSubscribedUser(String user) { //Adds another url corresponding with a matched document
		this.subscribedUsers.remove(user);
	}
	
	public void clearMatches() { //used by the channel matching bolt
		this.documentURLs.clear();
	}
}
