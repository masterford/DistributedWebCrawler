package edu.upenn.cis455.storage;

import java.io.Serializable;
import java.util.ArrayList;

/*Custom class to store user values e.g. password and searches  */
public class UserVal implements Serializable {
	
	private byte[] hashedPassword;	
	private String firstName;
	private String lastName;
	private ArrayList<String> subscribedChannels; //channels the user is subscribed to
	private ArrayList<String> createdChannels; //channels the user created
	private static final long serialVersionUID = 7526471155622776148L;
	
	public UserVal(String firstName, String lastName, byte[] hashedPassword) {
		this.firstName = firstName;
		this.lastName = lastName;
		this.hashedPassword = hashedPassword;
		subscribedChannels = new ArrayList<String>();
		createdChannels = new ArrayList<String>();
	}
	
	public byte[] getHashedPassword() {
		return this.hashedPassword;
	}
	
	public String getFirstName() {
		return this.firstName;
	}
	
	public String getLastName() {
		return this.lastName;
	}
	
	public ArrayList<String> getCreatedChannels(){
		return this.createdChannels;
	}
	
	public ArrayList<String> getSubscribedChannels(){
		return this.subscribedChannels;
	}
	
	public void addSubscription(String channelName) {
		this.subscribedChannels.add(channelName);
	}
	
	public void removeSubscription(String channelName) {
		this.subscribedChannels.remove(channelName);
	}
	
	public void removeCreatedChannel(String channelName) {
		this.createdChannels.remove(channelName);
	}
	
	public void addCreatedChannel(String channelName) {
		this.createdChannels.add(channelName);
	}
}
