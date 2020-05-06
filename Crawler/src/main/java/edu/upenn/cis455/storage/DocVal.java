package edu.upenn.cis455.storage;

import java.io.Serializable;
import java.util.Date;

/*Class to store information about documents  */
public class DocVal implements Serializable {
	private String body;	//content body
	private int contentLength;
	private String contentType;
	private static final long serialVersionUID = 7526471155622776145L;
	private Date lastChecked;
	
	public DocVal(int contentLength, String contentType, String body, Date lastChecked) {
		this.contentLength = contentLength;
		this.contentType = contentType;
		this.body = body;
		this.lastChecked = lastChecked;
	}
	
	public String getBody() {
		return this.body;
	}
	
	public String getContentType() {
		return this.contentType;
	}
	
	public Date getLastChecked() { //returns the last time this document was checked by the crawler
		return this.lastChecked;
	}
	
	public void setLastChecked(Date date) {
		this.lastChecked = date;
	}
	
	public int getContentLength() {
		return this.contentLength;
	}
}
