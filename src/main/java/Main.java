package main.java;
import java.io.*;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import main.java.Record;

class Example implements Serializable {
    int value;

    public Example(int value, Example ref) {
        this.value = value;
    }
}

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Example y = new Example(2, null);
//        Example x = new Example(1, y);
       Example x = y;

        // Serialize x
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
       ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("test.ser"));
	   out.writeObject(x);
	   y.value =3;
	   
	   ///------------
	   ObjectInputStream in = new ObjectInputStream(new FileInputStream("test.ser"));
		Example r =  (Example) in.readObject();
		
		System.out.println(r.value);
		System.out.println(x.value);
	   

        // Deserialize x
        

        // Verify that deserializedX still references the original y object
//        System.out.println(deserializedX.ref.value); // Output: 2
    }
	public Iterator parseSQL(StringBuffer strbufSQL) throws DBAppException {
	    String sql = strbufSQL.toString().trim();
	    String[] tokens = sql.split("\\s+");
	    String command = tokens[0].toUpperCase();
	    
	    if (command.equals("CREATE")) {
	        String tableName = tokens[2];
	        String clusteringKeyColumn = tokens[4];
	        Hashtable<String, String> colNameType = new Hashtable<>();
	        Hashtable<String, String> colNameMin = new Hashtable<>();
	        Hashtable<String, String> colNameMax = new Hashtable<>();
	        
	        // extract column definitions
	        int i = 5;
	        while (i < tokens.length && !tokens[i].equals("PRIMARY")) {
	            String colName = tokens[i];
	            String colType = tokens[i + 1];
	            String colMin = tokens[i + 2];
	            String colMax = tokens[i + 3];
	            colNameType.put(colName, colType);
	            colNameMin.put(colName, colMin);
	            colNameMax.put(colName, colMax);
	            i += 4;
	        }
	        
	        // handle primary key
	        if (!tokens[i].equals("PRIMARY") || !tokens[i + 1].equals("KEY")) {
	            throw new DBAppException("Invalid CREATE statement");
	        }
	        String primaryKey = tokens[i + 2];
	        
	        // create table
	        createTable(tableName, clusteringKeyColumn, colNameType, colNameMin, colNameMax);
	    }
	    else if (command.equals("INSERT")) {
	        String tableName = tokens[2];
	        Hashtable<String, Object> colNameValue = new Hashtable<>();
	        
	        // extract column names and values
	        int i = 4;
	        while (i < tokens.length-1 && !tokens[i].equals(")")) {
	            String colName = tokens[i].replace(",", "");
	            Object colValue = null;
	            if (tokens[i+1].charAt(0) == '\'') {
	                colValue = tokens[i + 1].substring(1, tokens[i + 1].length() - 1);
	            } else {
	                colValue = (tokens[i + 1]);
	            }
	            colNameValue.put(colName, colValue);
	            i += 2;
	        }
	        
	        // insert into table
	        System.out.println(tableName);
	        System.out.println(colNameValue);
	        insertIntoTable(tableName, colNameValue);
	    }
	    else {
	        throw new DBAppException("Unsupported SQL statement");
	    }
	    
	    return null;
	}
}
