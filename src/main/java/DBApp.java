package main.java;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
public class DBApp {
	private int maxRowPerPage;
	
	
	public void init() {
		try {
			Properties prop = new Properties();
			InputStream input = new FileInputStream("src/main/resources/DBApp.config") ;
			prop.load(input);
			File metadata = new File("src/main/resources/metadata.csv");
			metadata.createNewFile();
			maxRowPerPage = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
		}
		catch(Exception e) {
		}
	}
	
	public void createTable(String strTableName,String strClusteringKeyColumn,Hashtable<String,String> htblColNameType,Hashtable<String,String> htblColNameMin,
							Hashtable<String,String> htblColNameMax) throws DBAppException 
	{
		//Checks
		if(checkIfTableExists(strTableName) == true || checkDataTypeAndKey(strClusteringKeyColumn, htblColNameType) == false)
			throw new DBAppException("Table exists or wrong data types");
		//Write in metadata file
		Set<String> keys1 = htblColNameMax.keySet();
		Set<String> keys2 = htblColNameMin.keySet();
		Set<String> keys3 = htblColNameType.keySet();

		if (!(keys1.equals(keys2) && keys2.equals(keys3)))
			throw new DBAppException("Columns don't match");
//
		try {
			FileWriter fileWriter = new FileWriter("src/main/resources/metadata.csv",true);
			BufferedWriter csvWriter = new BufferedWriter(fileWriter);
			 Set <String> keys =htblColNameType.keySet();
			 if(keys.size() != htblColNameType.size() || keys.size() !=htblColNameMax.size() || keys.size() != htblColNameType.size()) {
				 csvWriter.close();
				 throw new DBAppException("test1");
			 }
//			 for (String s: keys) {
//				 //prob helper aw hanshelha
//				 if (Integer.parseInt(htblColNameMin.get(s)) > Integer.parseInt(htblColNameMax.get(s)))
//					 throw new DBAppException("Min and max values don't correspond");
//					 
//			 }
			 for (String s : keys) {
				csvWriter.write(strTableName+",");
				csvWriter.write(s+",");
				csvWriter.write(htblColNameType.get(s)+",");
				csvWriter.write((strClusteringKeyColumn.equals(s)?"True":"False")+",");
				csvWriter.write("null,");
				csvWriter.write("null,");
				csvWriter.write(htblColNameMin.get(s)+",");
				csvWriter.write(htblColNameMax.get(s)+",");
				csvWriter.write("\n");
			}
			 csvWriter.flush();
			 csvWriter.close();
			 //Create a new Table Object
			 Table table = new Table(0,strTableName);
			 table.setColumns(keys.size());
			 ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" +strTableName +".ser"));
			 out.writeObject(table);
			 out.close(); 
		} 
		catch (IOException e) {	
			throw new DBAppException(e.getMessage());
		}
		
	}
	
	public void insertIntoTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException{
		//1 get primaryKey columnName from metadata DONE
		// check that it is not missing htblColNameValue (not null) DONE
		// check if primary key unique (delay) BinarySearch
		//2 matching datatypes with metadata file (date format,min,max)
		ArrayList <String> TypeAndPk = primaryKeyInfoAndCheck(strTableName, htblColNameValue,true);
		//3 First Insert/handle full page
		//4 Update number of rows,create new record,insert in page if not full
		//5 check missing insertion
		checkDataTypes(strTableName, htblColNameValue);
		int [] index = binarySearchMadeByUs(htblColNameValue.get(TypeAndPk.get(1)), TypeAndPk, strTableName);
		if(index[0] != -1)
			throw new DBAppException();
		ObjectInputStream in;
		try {
			in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + strTableName + ".ser"));
			Table table = (Table) in.readObject();
			Set <String> hs = htblColNameValue.keySet(); 
			if(table.getPages().size()==0){
				Vector <Record> newPage = new Vector <Record>();
				table.getPages().add(1);
				newPage.add(new Record(htblColNameValue));
				ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName +  "1.ser"));
				outputStream.writeObject(newPage);
				outputStream.close();
			}
			else {
				Vector <Record> currentPage = deserializePage(table, index[1]);
				currentPage.add(index[2],new Record(htblColNameValue));
				//test on the index
				if(currentPage.size() > maxRowPerPage) {
					Vector <Record> newPage;
					if(table.getPages().contains(index[1]+1)) {
						newPage = deserializePage(table, index[1]+1);
					}else {
					table.getPages().add(table.getPages().size()+1);
					newPage = new Vector <Record>();}
					newPage.add(0,currentPage.lastElement());
					currentPage.remove(currentPage.size()-1);					
					ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + (index[1] +1) + ".ser"));
					ObjectOutputStream outputStream2 = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + index[1]+  ".ser"));
					outputStream.writeObject(newPage);
					outputStream2.writeObject(currentPage);
					outputStream.close();
					outputStream2.close();
					serializeTable(table);
					return;	
				}
				ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + (table.getPages().size()) +  ".ser"));
				outputStream.writeObject(currentPage);
				outputStream.close();
			}
			table.setRows(table.getRows() +1);
			in.close();
			serializeTable(table);
		} catch (Exception e) {
//			e.printStackTrace();
			throw new DBAppException(e.getMessage() + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		}
		
	}
	
	public void updateTable(String strTableName,String strClusteringKeyValue,Hashtable<String,Object> htblColNameValue ) throws DBAppException{
		
		//1 Check Table name exists
		try {
			if(htblColNameValue == null)
				return;
			ObjectInputStream in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + strTableName + ".ser"));
			Table table = (Table) in.readObject();
			checkIfTableExists(strTableName);
			//2 Check Datatypes, all Columns exists
			checkDataTypes(strTableName, htblColNameValue);
			
			//3 Check existing primary Key, Search for the record using clusteringKey
			ArrayList <String> pkInfo = primaryKeyInfoAndCheck(strTableName, htblColNameValue,false);
			Object pk = parsePrimaryKey(pkInfo.get(0),strClusteringKeyValue);
//			System.out.println(pk);
			htblColNameValue.put(pkInfo.get(1), pk);
			int [] index = binarySearchMadeByUs(htblColNameValue.get(pkInfo.get(1)), pkInfo, strTableName);

			if(index[0] == -1) {
					in.close();
					throw new DBAppException("No record found");
			}
			
			Vector <Record> currentPage = deserializePage(table, index[1]);
			Record oldValue = currentPage.remove(index[0]); //worst case
			Set <String> oldie = oldValue.getV().keySet();
			Record newie = new Record(htblColNameValue);
			for(String s : oldie) {
				if(newie.getV().get(s)==null)
					newie.getV().put(s, oldValue.getV().get(s));
			}
			currentPage.add(index[0],new Record(htblColNameValue));
			ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + index[1] + ".ser"));
			outputStream.writeObject(currentPage);
			outputStream.close();	
			in.close();
		}
		catch(Exception e) {
			//			e.printStackTrace(); StackTrace();
			throw new DBAppException(e.getMessage());
		}
		//5 worst case scenario  
	}
			
	private Object parsePrimaryKey(String type, String strClusteringKeyValue) throws ParseException {
		Object res = strClusteringKeyValue;
		type = type.substring(10);
		if(type.equals("Integer")) {
			res = Integer.parseInt(strClusteringKeyValue);
		}
		else if(type.equals("Double")) {
			res = Double.parseDouble(strClusteringKeyValue);
					}
		else if(type.equals("Date")) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			res = dateFormat.parse(strClusteringKeyValue);	
		}
		return res;
	}


	public void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException{
		//1 check if table exists
		//2 check col exists
	    //3 search linearly through pages and compare each record
		//3.1 if it matches, delete. If page is empty delete the page
		//if no records found mat3mlsh haga
		try {
		if (checkIfTableExists(strTableName) == false)
			throw new DBAppException("No such table");
		checkDataTypes(strTableName, htblColNameValue);
		ObjectInputStream in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + strTableName + ".ser"));
		Table table = (Table) in.readObject();
		for(int i = 0; i<table.getPages().size();i++) {
			Vector <Record> page = deserializePage(table, table.getPages().get(i));
			for(int j = 0;j<page.size();j++) {
				if(page.get(j).compareRecords(htblColNameValue) == true) {
					page.remove(j);
					j--;}
			}
			ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + table.getPages().get(i) +".ser"));
			outputStream.writeObject(page);
			outputStream.close();
			in.close();
			if(page.size() == 0){
				File file = new File("src/main/resources/data/" + strTableName + table.getPages().get(i) +".ser");
				System.out.println(file.delete());
				table.getPages().remove(i);
				i--;
				serializeTable(table);
			}
			page = null;
			System.gc();
		}
		}
		catch(Exception e) {
			throw new DBAppException(e.getMessage());
		}		
	}

	private boolean checkIfTableExists(String tableName) throws DBAppException{
		BufferedReader br = null;
		FileReader fr = null;
		try {
			 fr = new FileReader("src/main/resources/metadata.csv");
			 br = new BufferedReader(fr);
			String s = br.readLine();
			while(s != null) {
				if(s.split(",")[0].equals(tableName)) 
				    return true;
				s = br.readLine();
			}
			br.close();
			fr.close();
			return false;
		} catch (Exception e) {
			throw new DBAppException("Table Already Exists");
		}
		
	}
	
	//Check DataTypes are in the 4 given types , pk exists
	private boolean checkDataTypeAndKey(String clusteringKey,Hashtable <String,String> col) {
		Set <String> s = col.keySet();
		ArrayList <String> ar = new  ArrayList <String>();
		ar.add("java.lang.Integer");
		ar.add("java.lang.String");
		ar.add("java.lang.Double");
		ar.add("java.util.Date");
		boolean keyExists = false;
		for(String x : s ) {
			if(!ar.contains(col.get(x)))
				return false;
			if(x.equals(clusteringKey))
				keyExists = true;
		}
		return keyExists;
	}
	
	//returns arraylist datatype of pk,pk name
	private ArrayList <String> primaryKeyInfoAndCheck(String strTableName,Hashtable<String,Object> htblColNameValue,boolean insert) throws DBAppException {
		
		try {
			FileReader fr = new FileReader("src/main/resources/metadata.csv");
			BufferedReader br = new BufferedReader(fr);
			ArrayList <String> res = new ArrayList<String>(); 
			String s = br.readLine();
			String pk = "";
			while(s != null) {
				if(s.split(",")[0].equals(strTableName) && s.split(",")[3].equals("True")) {
					pk = s.split(",")[1]; //got the primary key
					res.add(s.split(",")[2]);
					break;
				}
				s = br.readLine();	
			}
			if((pk.equals("") || htblColNameValue.get(pk)==null) && insert) {
				br.close();
				throw new DBAppException("noPk");
			}
			res.add(pk);
			br.close();
			return res;	
		} catch (Exception e) {
			throw new DBAppException(e.getMessage());
		}
	}
	
	private boolean checkDataTypes(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException{
		try {
			
			Set <String> cols = htblColNameValue.keySet();
			for(String s: cols) {
				boolean flagAfashnaElColumn = false;
//				boolean flagEnter = false;
				FileReader fr = new FileReader("src/main/resources/metadata.csv");
				BufferedReader br = new BufferedReader(fr);
				String st = br.readLine();
		
				while(st!=null) {

					String [] split = st.split(",");
					if(split[1].equalsIgnoreCase(s) && split[0].equals(strTableName)) {
						flagAfashnaElColumn = true;
						
						if(!split[2].equals(htblColNameValue.get(s).getClass().toString().substring(6))) {
							br.close();
//							System.out.println(st);
//							System.out.println(htblColNameValue.get(s).getClass().toString().substring(6));
				         	throw new DBAppException("wrong datatype");
						}
						String type = split[2].substring(10);
						boolean typeTmam =true;
						if(type.equals("Integer")) {
							typeTmam = Integer.parseInt(htblColNameValue.get(s).toString()) >= Integer.parseInt(split[6]) &&
									Integer.parseInt(htblColNameValue.get(s).toString()) <= Integer.parseInt(split[7]);
						}
						else if(type.equals("String")) {
							typeTmam = htblColNameValue.get(s).toString().compareTo(split[6]) >=0 &&
									htblColNameValue.get(s).toString().compareTo(split[7]) <=0;
						}
						else if(type.equals("Double")) {
							typeTmam = Double.parseDouble(htblColNameValue.get(s).toString()) >= Double.parseDouble(split[6]) &&
									Double.parseDouble(htblColNameValue.get(s).toString()) <= Double.parseDouble(split[7]);
						}
						else if(type.equals("Date")) {
							SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
							typeTmam = ((Date )  htblColNameValue.get(s)).compareTo(dateFormat.parse(split[6])) >= 0 &&
									 ((Date )  htblColNameValue.get(s)).compareTo(dateFormat.parse(split[7])) <= 0;
							
						}
						if(!typeTmam) {
							System.out.println(split[2]);
							br.close();
							throw new DBAppException("error");
							}
					}
					st = br.readLine();
				}
				if(flagAfashnaElColumn == false) {
					br.close();
					throw new DBAppException("no column with this name");
				}
				br.close();
			}
			
			return true;
		} catch (Exception e) {			
			throw new DBAppException(e.getMessage());
		}
		
	}
	
	private int [] binarySearchMadeByUs(Object target, ArrayList <String> pkInfo, String tableName) throws DBAppException {
		int [] res = new int [3];
		boolean flag = false;
		try {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + tableName + ".ser"));
		Table table = (Table) in.readObject();
		in.close();
		if(table.getPages().size()==0) {
			res[0] = -1;
			res[1] = -1;
			res[2] = -1;
			return res;
		}
		int low = 0;
        int i = 0;
        int mid = 0;
        for(i =0; i<table.getPages().size();i++) {
             ObjectInputStream pageIn = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + tableName + table.getPages().get(i) + ".ser"));
             Vector <Record> vector = (Vector <Record> ) pageIn.readObject();
             low = 0;
             mid = 0;
	        int high = vector.size() - 1;
	        while (low <= high) {
	            mid = (low + high) / 2;
	            Object current = vector.get(mid).getV().get(pkInfo.get(1));
	            if  (((Comparable<Object>)current).compareTo(target) == 0) {
	            	res[0] = mid; //makan el feh row
	            	res[1] = table.getPages().get(i); // rakm page
//	            	System.out.println("sdfkjdskjfhkjsdbfkj");
	            	res[2] = mid; // makan el feh row
	            	in.close();
	            	pageIn.close();
	                return res;
	            } else if (((Comparable<Object>)current).compareTo(target) < 0) {
	                low = mid + 1;
	            } 
	            else {
	                high = mid - 1;
	            	}
	        	}
	        pageIn.close();
	        if((((Comparable<Object>)vector.lastElement().getV().get(pkInfo.get(1))).compareTo(target) > 0)) {
	        	flag = true;
	        	break;
	        }
	        pageIn = null;
	        System.gc();
	        }        	
	        res[0] = -1;
			res[1] =(flag == false? table.getPages().get(i-1): table.getPages().get(i));
			res[2] = low;
//			in.close();
			return res;
        }
		catch(Exception e) {
//						e.printStackTrace();
		 throw new DBAppException("bs");
		}
	}

	private void serializeTable (Table table) throws FileNotFoundException, IOException {
		 ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + table.getTableName() +".ser"));
		 out.writeObject(table);
		 out.close();
	}
	

	private Vector <Record> deserializePage (Table table, int pageNumber) throws FileNotFoundException, IOException, ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + table.getTableName() + pageNumber + ".ser"));
		Vector <Record> r =  (Vector <Record>) in.readObject();
		in.close();
		return r;
	}
	
	public static void main(String[] args) throws DBAppException, FileNotFoundException, IOException, ClassNotFoundException {
		DBApp x = new DBApp();
		x.init();
		
//		ObjectInputStream in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + "pcs.ser"));
//		
//	    Table table = (Table) in.readObject();
	    
	    Hashtable <String,String>  htblColNameType = new Hashtable <String,String> ( );
		Hashtable <String,String>  htblColNameMin = new Hashtable <String,String> ( );
		Hashtable <String,String>  htblColNameMax = new Hashtable <String,String> ( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");	    
		htblColNameMin.put("id", "1");
		htblColNameMin.put("name", "a");
		htblColNameMin.put("gpa", "1");
		htblColNameMax.put("id", "1000000");
		htblColNameMax.put("name", "zzzzzzzzzzzzzzzz");
		htblColNameMax.put("gpa", "4");
		Hashtable <String,Object> htblColNameValue = new Hashtable( );
//	    htblColNameValue.put("id", new Integer(15)); 
	    htblColNameValue.put("name",new String("zakya shwaya")); 
//	    htblColNameValue.put("gpa", new Double(3.0)); 
		
//	    x.insertIntoTable("blabla", htblColNameValue);
//	    x.updateTable("blabla", "1", htblColNameValue);
//	    x.deleteFromTable("blabla", htblColNameValue);
	    
//		Hashtable <String,Object> htblColNameValue = new Hashtable( );
//	    htblColNameValue.put("id", new String( "99-9010" )); 
//	    htblColNameValue.put("first_name", new String("Zaky" ) ); 
//	    htblColNameValue.put("last_name", new String("noor" ) ); 
//	    htblColNameValue.put("gpa", new Double( 3.00 ) ); 
//	    htblColNameValue.put("dob",  new Date(1999 - 1900, 4 - 1, 1));
//	    x.insertIntoTable( table.getTableName() , htblColNameValue ); 
//		x.deleteFromTable(table.getTableName(), htblColNameValue);
//		
		
		
//		x.updateTable("pcs", "4230", test);
//		Vector <Record> v = x.deserializePage(table, 1);
//		System.out.println(v.size());
//		for(int i = 0;i<v.size();i++) {
//			System.out.println(v.get(i).getV().get("id"));
//		}
//		System.out.println();
//		Vector <Record> v2 = x.deserializePage(table, 2);
//		System.out.println(v2.size());
//		for(int i = 0;i<v2.size();i++) {
//			
//			System.out.println(v2.get(i).getV().get("id"));
//		}
		
		
//		Hashtable <String,String>  htblColNameMin = new Hashtable <String,String> ( );
//		Hashtable <String,String>  htblColNameMax = new Hashtable <String,String> ( );
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.Double");
//		htblColNameMin.put("id", "1");
//		htblColNameMin.put("name", "a");
//		htblColNameMin.put("gpa", "1");
//		htblColNameMax.put("id", "1000000");
//		htblColNameMax.put("name", "zzzzzzzzzzzzzzzz");
//		htblColNameMax.put("gpa", "4");
//		x.createTable("hassan","id",htblColNameType,htblColNameMin,htblColNameMax);
//		System.out.println(x.checkIfTableExists("transcripts"));
		
//		Hashtable <String,String>  htblColNameType = new Hashtable <String,String> ( );
//		Hashtable <String,String>  htblColNameMin = new Hashtable <String,String> ( );
//		Hashtable <String,String>  htblColNameMax = new Hashtable <String,String> ( );
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.Double");
//		Vector <Integer> v = new Vector<Integer>();
//		v.add(2);
//		v.add(3);
//		v.add(4);
//		v.add(1, 5);
//		System.out.println(v);
		// 2 5 3 4
//		System.out.println(htblColNameType.get("b"));
		
//	

//		x.createTable( "test", "id", htblColNameType,htblColNameMin,htblColNameMax);
//		x.createTable( "test2", "id", htblColNameType,htblColNameMin,htblColNameMax);
//		x.createTable(null, null, null, null, null);
		
		try {
			ObjectInputStream inp = new ObjectInputStream(new FileInputStream("src/main/resources/data/students.ser"));
			Table b = (Table) inp.readObject();
			System.out.println(b.getTableName());
			System.out.println(b.getPages());
			System.out.println(b.getRows());
			Vector <Record>  v = x.deserializePage(b, 1);
			System.out.println(v);
			Vector <Record>  v2 = x.deserializePage(b, 2);
			System.out.println(v2);
		}
		catch(Exception e)
		{}
	}

	
	
	

}