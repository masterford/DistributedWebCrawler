/*
 * @author: Ransford Antwi
 */

package edu.upenn.cis455.frontend;

import static spark.Spark.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.net.URLDecoder;

import edu.upenn.cis455.storage.*;
import edu.upenn.cis455.xpathengine.XPathEngineFactory;
import edu.upenn.cis455.xpathengine.XPathEngineImpl;

class XPathApp {
	public static void main(String args[]) {

    if (args.length != 1) {
      System.err.println("You need to provide the path to the BerkeleyDB data store!");
      System.exit(1);
    }
  
    port(8080);
    StorageServer.getInstance().init(args[0]);
    XPathEngineImpl engine = (XPathEngineImpl) XPathEngineFactory.getXPathEngine();
    
    get("/", (request,response) -> {
    	String fullName = (String)(request.session().attribute("fullname"));
        if (fullName == null) { //user not logged in
          return "<html>"
          		+	 "<body>"
          			+ "Please log in:"
          				+ "<form action=\"/login\" method=\"POST\">"
          					+ "<p>username: <input type=\"text\" name=\"username\"/ required></p>"
          					+ "password: <input type=\"text\" name=\"password\" required/>"
          					+ "<input type=\"submit\" value=\"Log in\"/>"
          				+ "</form>"
          			+ "<a href=\"/newaccount\">Create Account</a>"
          			+ "</body>"
          	 + "</html>";
        } else { //user logged in
        	String userName = (String)(request.session().attribute("username"));
        	StringBuilder body = new StringBuilder();
        	HashMap<String, ChannelStorage> all = StorageServer.getInstance().getAllChannels();
        	
        	/*Display list of channels  */
        	body.append("<html><body><p>Here are a list of all the channels in the system </p><ul>"); 
        	for(String channel : all.keySet()) {
        		String line = "";
        		if(all.get(channel).getSubscribedUsers().contains(userName)) { //user is subscribed to this channel 
        			if(all.get(channel).getOwner().equals(userName)) { //user is subscribed to this channel and is the owner
        			   line = String.format("<li> %s <a href=\"/show?name=%s\">View Channel</a>  <a href=\"/unsubscribe?name=%s\">Unsubscribe</a>  <a href=\"/delete?name=%s\">Delete Channel</a> </li>", channel, channel,channel,channel);
        			}else {
        				line = String.format("<li> %s <a href=\"/show?name=%s\">View Channel</a>  <a href=\"/unsubscribe?name=%s\">Unsubscribe</a></li>", channel, channel,channel);
        			}
        		} else if(all.get(channel).getOwner().equals(userName)) { //user is the owner but not subscribed to the channel
        			line = String.format("<li> %s <a href=\"/subscribe?name=%s\">Subscribe</a>  <a href=\"/delete?name=%s\">Delete</a></li>", channel, channel,channel);
        		} else {
        			line = String.format("<li> %s <a href=\"/subscribe?name=%s\">Subscribel</a></li>", channel, channel);
        		}
        		body.append(line);        		
        	}           	
        	body.append("<br>");
        	/*Create channel form  */
        	String form = "Create a New Channel:"
          				+ "<form action=\"/create\" method=\"GET\">"
          					+ "<p>Channel Name: <input type=\"text\" name=\"name\"/ required></p>"
          					+ "Xpath: <input type=\"text\" name=\"xpath\" required/>"
          					+ "<input type=\"submit\" value=\"Create\"/>"
          				+ "</form>";    
        	body.append(form);
        	body.append("</ul><p><a href=\"/logout\">Logout</a></body></html>");
        	body.append("</body></html>");
          return body.toString();
        }
    	//return "Hello world!<p><a href=\"/login\">Go to the login page</a>";
    });
    

    /* Displays a create account form creates a new account in the DB if valid */
    get("/newaccount", (request, response) -> {   
    	return "<html>"
          		+	 "<body>"
          			+ "Create Account:"
          				+ "<form action=\"/register\" method=\"POST\">"
          					+ "<p>username: <input type=\"text\" name=\"username\"/ required> </p>"
          					+ "<p>FirstName: <input type=\"text\" name=\"firstname\"/ required> </p>"
          					+ "<p>LastName: <input type=\"text\" name=\"lastname\"/ required> </p>"
          					+ "password: <input type=\"text\" name=\"password\" required/>"
          					+ "<input type=\"submit\" value=\"Register\"/>"
          				+ "</form>"
          			+ "<a href=\"/\">Back</a>"
          			+ "</body>"
          	 + "</html>";
      });
    
    /* Receives the data from the create account form, checks if the username already exists in DB, and redirects the user back to
    /. If the username already exists, send an error page, else add username, first+last name and hashed password to BerkeleyDB */
    /*
    post("/register", (request, response) -> {
        String username = request.queryParams("username");
        // Check BerkeleyDB
        if(StorageServer.getInstance().exists(username)) { //username already exists
        	return "<html><body>Username already in use<p><a href=\"/newaccount\">Back</a></body></html>";
        }
        
        String firstName = request.queryParams("firstname");
        String lastName = request.queryParams("lastname");
        String password = request.queryParams("password");
        
        if(firstName == null || lastName == null || password == null) {
        	return "<html><body>Fields cannot be Null<p><a href=\"/newaccount\">Back</a></body></html>";
        }
        
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(password.getBytes(StandardCharsets.UTF_8));
        UserVal userInfo = new UserVal(firstName, lastName, hash);
        StorageServer.getInstance().addUserInfo(username, userInfo);
        response.redirect("/");
        return null;
      }); */
    
    /* Receives the data from the home page, logs the user in, and redirects the user back to
       /. If invalid login details, return error page. */
    /*
    post("/login", (request, response) -> {
      String username = request.queryParams("username");
      String password = request.queryParams("password");
      
      if(username == null || password == null) {
    	  return "<html><body>Fields cannot be null<p><a href=\"/\">Back</a></body></html>";
      }
      //Check if username and hashed password matches the user's hash in BerkeleyDB
      UserVal userInfo = StorageServer.getInstance().getUserInfo(username);
      
      if(userInfo == null) {
    	  return "<html><body>Username does not exist<p><a href=\"/\">Back</a></body></html>";
      }
      
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(password.getBytes(StandardCharsets.UTF_8));
      byte[] hashed = userInfo.getHashedPassword();
      
      if(!Arrays.equals(hashed, hash)) {
    	  return "<html><body>Invalid Password<p><a href=\"/\">Back</a></body></html>";
      }
      request.session().attribute("username", username);
      request.session().attribute("fullname", userInfo.getFirstName() + " " + userInfo.getLastName());     
      response.redirect("/");
      return null;
    }); */

    /* Logs the user out by deleting the "username" attribute from the session. You could also
       invalidate the session here to get rid of the JSESSIONID cookie entirely. */

    get("/logout", (request, response) -> {
      request.session().invalidate();
      response.redirect("/");
      return null;
    });	
    
    get("/lookup", (request, response) -> {
       String raw_url = request.queryParams("url");
       if(raw_url == null) {
    	   response.status(400);
    	   return null;
       }       
       String url = URLDecoder.decode(raw_url, "UTF-8");
       
       DocVal documentInfo = StorageServer.getInstance().getDocInfo(url);
       if(documentInfo == null) {
    	   response.status(404); //doc doesn't exist
    	   return null;
       }       
       response.type(documentInfo.getContentType());
       return documentInfo.getBody();
       
      });	
    
    /*User Interface */
    /*
    get("/create", (request, response) -> {
    	if(request.session().attribute("username") == null) { //no user logged in
    		response.status(401);
    		return "<html><body>Please log in First <p><a href=\"/\">Go to the Home page</a></p></body></html>";
    	}
    	String channelName =  request.queryParams("name");
    	String xpath = request.queryParams("xpath");
    	if(channelName == null || xpath == null) {
    	   response.status(400);
     	   return "<html><body>Channel name or Xpath cannot be Null<br><p><a href=\"/\">Home Page</a></p></body></html>";
    	}
    	if(!engine.isValidXPath(xpath)) {
    		response.status(400);
    		return String.format("<html><body>The String: %s is not a valid XPath Expression <p><a href=\"/\">Home Page</a></p>", xpath);
    	}
    	
    	String owner = request.session().attribute("username");
    	ChannelStorage newChannel = new ChannelStorage(channelName, xpath, owner);
    	System.out.println(newChannel.getXPath());
    	if(StorageServer.getInstance().getChannelInfo(channelName) != null) { //channel already exists
    		response.status(409);
    		return "<html><body>Channel already exists</body><p><a href=\"/\">Home Page</a></p></html>";
    	}else {
    		UserVal user = StorageServer.getInstance().getUserInfo(owner);
    		user.addCreatedChannel(channelName);
    		StorageServer.getInstance().addUserInfo(owner, user);
    		StorageServer.getInstance().addChannelInfo(channelName, newChannel);
    		
    		String body = String.format("<html><body>You have successfully created Channel %s: <br><p><a href=\"/\">Home Page</a></p></body></html>", channelName);
        	return body;
    	}
    }); */
    
    /*
    get("/delete", (request, response) -> {
    	if(request.session().attribute("username") == null) { //no user logged in
    		response.status(401);
    		return "<html><body>Please log in First <p><a href=\"/\">Go to the Home page</a></p></body></html>";
    	}
    	String channelName =  request.queryParams("name");
    	if(channelName == null) {
    	   response.status(400);
     	   return "<html><body>Channel name cannot be Null <p><a href=\"/\">Go to the Home page</a></p></body></html>";
    	}
    	String user = request.session().attribute("username");
    	ChannelStorage channel = StorageServer.getInstance().getChannelInfo(channelName);
    	if(channel == null) {
    		response.status(404); 
    		return "<html><body>Channel Not Found <p><a href=\"/\">Go to the Home page</a></p></body></html>";
    	}
    	String owner = channel.getOwner();
    	if(!owner.equals(user)) { //channel owner is different from logged in user
    		response.status(403);
    		return "<html><body>Channel can only be deleted by Owner <p><a href=\"/\">Go to the Home page</a></p></body></html>";
    	}
    	int result = StorageServer.getInstance().deleteChannel(channelName);
    	if(result == 0) {
    		response.status(500); //unable to delete channel
    		return "<html><body>A Problem was encountered whilst trying to delete this channel <p><a href=\"/\">Go to the Home page</a></p></body></html>";
    	}
    	//TODO: Remove this channel from every user's list of subscribed channels
    	HashMap<String, UserVal> users = StorageServer.getInstance().getAllUsers();
    	for(String userName : users.keySet()) {
    		UserVal val = users.get(userName);
    		if(val.getSubscribedChannels().contains(channelName)) {
    			val.removeSubscription(channelName);
        		StorageServer.getInstance().addUserInfo(userName, val);
    		}   		
    	}
    	return String.format("<html><body>You have successfully deleted Channel %s: <br><p><a href=\"/\">Home Page</a></p></body></html>", channelName);
    }); */
    
    /*
    get("/subscribe", (request, response) -> {
    	if(request.session().attribute("username") == null) { //no user logged in
    		response.status(401);
    		return "<html><body>Please log in First <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    	}
    	String channelName =  request.queryParams("name");
    	if(channelName == null) {
    	   response.status(400);
     	   return "<html><body>Cannot subscribe to an empty channel <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    	}
    	String user = request.session().attribute("username");
    	ChannelStorage channel = StorageServer.getInstance().getChannelInfo(channelName);
    	if(channel == null) {
    		response.status(404); 
    		return "<html><body>Channel Not Found <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    	}
    	UserVal userInfo = StorageServer.getInstance().getUserInfo(user);
    	if(userInfo.getSubscribedChannels().contains(channelName)) {
    		response.status(409); 
    		return "<html><body>You are already subscribed to this channel <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    	}
    	channel.addSubscribedUsers(user);
    	userInfo.addSubscription(channelName);
    	StorageServer.getInstance().addUserInfo(user, userInfo); //re add user info back to database  
    	StorageServer.getInstance().addChannelInfo(channelName, channel); //re add user info back to database  
    	String body = String.format("<html><body>You have successfully subscribed to Channel: %s <p><a href=\"/\">Go to the Home page</a></p</body></html>", channelName);
    	return body;
    });
    
    get("/unsubscribe", (request, response) -> {
    	if(request.session().attribute("username") == null) { //no user logged in
    		response.status(401);
    		return "<html><body>Please log in First <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    	}
    	String channelName =  request.queryParams("name");
    	if(channelName == null) {
    	   response.status(400);
     	   return "<html><body>Cannot unsubscribe from an empty channel <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    	}
    	String user = request.session().attribute("username");
    	ChannelStorage channel = StorageServer.getInstance().getChannelInfo(channelName);
    	if(channel == null) {
    		response.status(404); 
    		return "<html><body>Channel Not Found <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    	}
    	UserVal userInfo = StorageServer.getInstance().getUserInfo(user);
    	if(!userInfo.getSubscribedChannels().contains(channelName)) {
    		response.status(404); 
    		return "<html><body>You are not subscribed to this channel <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    	}
    	channel.removeSubscribedUser(user);
    	userInfo.removeSubscription(channelName);
    	StorageServer.getInstance().addUserInfo(user, userInfo); //re add user info back to database  
    	StorageServer.getInstance().addChannelInfo(channelName, channel); //re add channel back to the database
    	return "<html><body>You have successfully unsubscribed from this channel <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    }); */
    
    get("/show", (request, response) -> {
    	
    	if(request.session().attribute("username") == null) { //no user logged in
    		response.status(401);
    		return "<html><body>Please log in First <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    	}
    	String channelName =  request.queryParams("name");
    	if(channelName == null) {
    	   response.status(400);
     	   return "<html><body>Cannot Show an empty channel <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    	}
    	String user = request.session().attribute("username");
    	   	
    	ChannelStorage channel = StorageServer.getInstance().getChannelInfo(channelName);
    	if(channel == null) {
    		response.status(404); 
    		return "<html><body>Channel Not Found <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    	}
    	//UserVal userInfo = StorageServer.getInstance().getUserInfo(user);
    	if(!channel.getSubscribedUsers().contains(user)) {
    		response.status(404); 
    		return "<html><body>You are not subscribed to this channel <p><a href=\"/\">Go to the Home page</a></p</body></html>";
    	}
    	/*Show Channel */
        StringBuilder finalString = new StringBuilder();
        finalString.append("<html><body>");
        finalString.append("<div class=\"channelheader\">");
        String headerInfo = String.format("<p>Channel name: %s</p><p>, created by: %s</p>", channelName, channel.getOwner());
        finalString.append(headerInfo);
        SimpleDateFormat dateFormatter = new SimpleDateFormat("YYYY-MM-DD'T'hh:mm:ss");
        
        for(String url : channel.getDocumentURLs()) {
        	System.out.println(url);
        	DocVal doc = StorageServer.getInstance().getDocInfo(url);        	
        	if(doc == null) {
        		continue;
        	}
        	String dateString = dateFormatter.format(doc.getLastChecked());       	
        	String metaData = String.format("<p>Crawled on: %s</p><p>Location: %s</p>", dateString, url);
        	finalString.append(metaData);
        	finalString.append("<div class=\"document\">");
        	String content = doc.getBody();
        	if(doc.getContentType().contains("text/xml") || doc.getContentType().endsWith(".xml") || doc.getContentType().contains("application/xml") ||doc.getContentType().equals("xml")) { //xml document
        		content = "<xmp>" + content;
        		content = content + "</xmp>";
        	}       	
        	finalString.append(content);
        	finalString.append("</div>"); 
        	finalString.append("<br>");    
        }
        finalString.append("</div><p><a href=\"/\">Go to the Home page</a></p");
    	finalString.append("</body></html>");
    	response.body(finalString.toString());
    	return finalString.toString();
    });
          
  }
}