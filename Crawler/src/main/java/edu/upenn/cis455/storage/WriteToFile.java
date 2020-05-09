package edu.upenn.cis455.storage;

import java.io.File;

public class WriteToFile {

	public static void main(String[] args) {
		String [] alphabet = {"A", "B", "C", "D", "E", "F", "G", "H"};
		try {
			String directory = System.getProperty("user.dir")+ "/DistributedStorage/" + args[0];
			StorageServer.getInstance().init(directory);
			int index = Integer.parseInt(args[0].split("_")[1]);
			File dir = new File(directory + "upload");
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
