/**
*@author Ransford Antwi
*/
package edu.upenn.cis455.storage;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;

public class S3Upload {

	public static void main(String[] args) {
		/* initialize s3 client */
		String BUCKET_NAME = "cis455-group13-crawldata";	
		String directory =  System.getProperty("user.dir")+ "/DistributedStorage/" + args[0]; 
		JSONParser parser = new JSONParser();
		try {
			String file = System.getProperty("user.dir")+"/CrawlerConfigs/s3_config.json";
            Object obj = parser.parse(new FileReader(file));

            JSONObject jsonObj = (JSONObject) obj;
            
            JSONArray access_key = (JSONArray) jsonObj.get("accKey");
            JSONArray secret_key = (JSONArray) jsonObj.get("secKey");
            JSONArray session_token = (JSONArray) jsonObj.get("seshToken");
                                            
            String accessKey = access_key.toArray()[0].toString();
            String secretKey = secret_key.toArray()[0].toString();
            String sessionToken = session_token.toArray()[0].toString();
            
            AWSCredentials credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
            AmazonS3 s3client = AmazonS3ClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials))
					.withRegion(Regions.US_EAST_1)
					.build(); 
		
			s3client.putObject(BUCKET_NAME, args[0].split("/")[1], new File(directory)); 
			s3client.shutdown();
		} catch(AmazonS3Exception | IOException | ParseException e) {
			e.printStackTrace();
			System.exit(0);;
		}					    			
			System.exit(0);
	}

}
