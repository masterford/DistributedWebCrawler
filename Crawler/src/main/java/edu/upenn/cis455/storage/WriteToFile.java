package edu.upenn.cis455.storage;

public class WriteToFile {

	public static void main(String[] args) {
		String [] alphabet = {"A", "B", "C", "D", "E"};
		try {
			String directory = System.getProperty("user.dir")+ "/DistributedStorage/" + args[0];
			StorageServer.getInstance().init(directory);
			int index = Integer.parseInt(args[0].split("_")[1]);
			StorageServer.getInstance().writetoFile(directory + "/corpus" + alphabet[index] + "_0.txt");
		}catch (NumberFormatException e) {
			e.printStackTrace();
			System.exit(0);
		}finally {
			System.exit(0);
		}
	}

}
