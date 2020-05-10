package edu.upenn.cis455.storage;

import java.io.File;

public class WriteToFile {

	public static void main(String[] args) {
		String [] alphabet = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N"};
		try {
			String directory = System.getProperty("user.dir")+ "/DistributedStorage/";
			StorageServer.getInstance().init(directory);
			int index = Integer.parseInt(args[0]);
			File dir = new File(directory + "/upload");
			dir.mkdir();
			StorageServer.getInstance().writetoFile(directory + "/upload/corpus" + alphabet[index]);
		}catch (NumberFormatException e) {
			e.printStackTrace();
			System.exit(0);
		}finally {
			System.exit(0);
		}
	}

}
