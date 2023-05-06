package main.java;

import java.io.*;

public class StaticHelpers {
	    public static void editMetadata(String filename, String strTableName, String[] strarrColName, Object x [], Object [] y,Object [] z) throws IOException {
	        File inputFile = new File(filename);
	        File tempFile = new File("temp.csv");

	        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
	        BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

	        String line;
	        int i = 0;
	        while ((line = reader.readLine()) != null) {
	            String[] columns = line.split(",");

	            // Check if the conditions are met
				if(columns[0].equals(strTableName) && (columns[1].equals(strarrColName[0]) || columns[1].equals(strarrColName[1]) || columns[1].equals(strarrColName[2])))  {
						columns[4] = strarrColName[0]+strarrColName[1]+strarrColName[2]; columns[5] = "Octree";
						i++;
	            }

	            // Write the updated line to the temp file
	            writer.write(String.join(",", columns));
	            writer.newLine();
	        }

	        // Close the reader and writer
	        reader.close();
	        writer.close();

	        // Replace the input file with the temp file
	        inputFile.delete();
	        tempFile.renameTo(inputFile);
	    }
}