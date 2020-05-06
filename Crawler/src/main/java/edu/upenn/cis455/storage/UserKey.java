package edu.upenn.cis455.storage;

import java.io.Serializable;

/*Custom class to store user info as key */
public class UserKey implements Serializable {
	private String userName;
	private String firstName;
	private String lastName;
	private static final long serialVersionUID = 7526471155622776147L;
	
	public UserKey(String userName, String firstName, String lastName) {
		this.userName = userName;
		this.firstName = firstName;
		this.lastName = lastName;
	}
	
	public String getUserName() {
		return this.userName;
	}
	
	public String getFirstName() {
		return this.firstName;
	}
	
	public String getLastName() {
		return this.lastName;
	}
}
