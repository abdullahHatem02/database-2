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
//			maxRowPerPage=5;
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
			FileReader fr = new FileReader("src/main/resources/metadata.csv");
			BufferedReader br = new BufferedReader(fr);
			String st = br.readLine();
	
			while(st!=null) {
//				System.out.println("here");
				String [] split = st.split(",");
				if(split[0].equals(strTableName)) {
					if(htblColNameValue.get(split[1]) == null)
						htblColNameValue.put(split[1], new Null());
				}
				st= br.readLine();
				}
			br.close();
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
//				System.out.println(currentPage);
				//test on the index
				int i =1;
				
					
				if(currentPage.size() > maxRowPerPage) {
					Vector <Record> newPage;
					do {
					
					
					
					if(table.getPages().contains(index[1]+i)) {
						newPage = deserializePage(table, index[1]+i);
						
					}else {
					table.getPages().add(table.getPages().size()+1);
					newPage = new Vector <Record>();}
					newPage.add(0,currentPage.lastElement());
					currentPage.remove(currentPage.size()-1);					
					ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + (index[1] +i) + ".ser"));
					ObjectOutputStream outputStream2 = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + (index[1]+ i -1) +  ".ser"));
					outputStream.writeObject(newPage);
					outputStream2.writeObject(currentPage);
					outputStream.close();
					outputStream2.close();
					serializeTable(table);
//					table.setRows(table.getRows() +1);
//					System.out.println(currentPage);
//					System.out.println(newPage);
					currentPage = newPage;
					i++;
//					System.out.println(currentPage.size());
//					System.out.println(!table.getPages().contains(index[1]+i));
					System.out.println(table.getPages().contains(index[1]+i));
				}while( currentPage.size() > maxRowPerPage  );}
				else {
				ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + (table.getPages().lastElement()) +  ".ser"));
				outputStream.writeObject(currentPage);
				outputStream.close();}
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
//					throw new DBAppException("No record found");
					return;
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
		if(htblColNameValue.get(primaryKeyInfoAndCheck(strTableName, htblColNameValue, false).get(1)) == null) {
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
		else {
			ArrayList <String> pkInfo = primaryKeyInfoAndCheck(strTableName, htblColNameValue, false);
			int [] index = binarySearchMadeByUs(htblColNameValue.get(pkInfo.get(1)), pkInfo, strTableName);
		
				ObjectInputStream in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + strTableName + ".ser"));
				Table table = (Table) in.readObject();
				Vector <Record> page = deserializePage(table,index[1]);
				if(page.get(index[2]).compareRecords(htblColNameValue) == true) {
//					System.out.println("as");
					page.remove(index[2]);
					}
				ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + index[1] +".ser"));
				outputStream.writeObject(page);
				outputStream.close();
				in.close();
				if(page.size() == 0){
					File file = new File("src/main/resources/data/" + strTableName +index[1] +".ser");
					System.out.println(file.delete());
					for(int i = 0; i<table.getPages().size();i++) {
						if(table.getPages().get(i)==index[1])
							table.getPages().remove(i);}
					serializeTable(table);
				}
				page = null;
				System.gc();
			
//				System.out.println(index[2]);
			
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DBAppException(e.getMessage());
		}		
	}
	
//	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[] strarrOperators) throws DBAppException {
//		//handle law sqlterm fady
//		checkIfTableExists(arrSQLTerms[0]._strTableName);
//		String strTableName = arrSQLTerms[0]._strTableName;
//		return null;
//	}
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException  {
		//handle law sqlterm fady
		try {
		checkIfTableExists(arrSQLTerms[0]._strTableName);
		String strTableName = arrSQLTerms[0]._strTableName;
		ObjectInputStream in;
		in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + strTableName + ".ser"));
		Table table = (Table) in.readObject();
		Vector <Record> v = deserializePage(table, 1);
	    Vector <Vector <Record>> result = new Vector <Vector <Record>>();
	    for(int i =0;i<arrSQLTerms.length;i++)
	    	result.add(new Vector <Record>());
	    for (int i = 0; i < v.size(); i++) {
	        Record record = v.elementAt(i);
	        
	        boolean satisfiesTerms = false;
	        for (int j = 0; j < arrSQLTerms.length; j++) {
	            SQLTerm term = arrSQLTerms[j];
	            Object value = record.getV().get(term._strColumnName);
//	            System.out.println(value);
	            switch (term._strOperator) {
	                case ">":
	                    satisfiesTerms = ((Comparable) value).compareTo(term._objValue) > 0;
	                    break;
	                case ">=":
	                    satisfiesTerms = ((Comparable) value).compareTo(term._objValue) >= 0;
	                    break;
	                case "<":
	                    satisfiesTerms = ((Comparable) value).compareTo(term._objValue) < 0;
	                    break;
	                case "<=":
	                    satisfiesTerms = ((Comparable) value).compareTo(term._objValue) <= 0;
	                    break;
	                case "!=":
	                    satisfiesTerms = !value.equals(term._objValue);
	                    break;
	                case "=":
	                    satisfiesTerms = value.equals(term._objValue);
	                    break;
	            }
	            if (satisfiesTerms) {
		            result.get(j).add(record);
		        }
	            if (!satisfiesTerms) {
//	                break; // short circuit evaluation
	            }
	        }
	        
	        
	    }
	    //Gebna results el bet satisfy ay condition
	    //result1//result2//result3
//	    for(int i = 0;i<result.size();i++)
//	    	System.out.println(result.get(i));	
	    if (strarrOperators == null) {
	        return result.iterator();
	    } else {
	    	Vector <Record> resultFinal = new Vector <Record>();
	    	
	    	for(int i =0;i<strarrOperators.length;i++) {
//	    		System.out.println(result.size());
	    		Vector <Vector <Record>> resultInt = new Vector <Vector <Record>>();
	    		resultInt.add(result.get(0));
	    		resultInt.add(result.get(1));
	    		result.remove(0);
	    		result.remove(0);
	    		if(strarrOperators[i] == "AND") {
	    		 result.add(0,intersect(resultInt.get(0),resultInt.get(1)));
	    		}
	    		else if(strarrOperators[i] == "OR") {
	    			result.add(0,union(resultInt.get(0),resultInt.get(1)));
		    		}
	    		else result.add(0,symmetricDifference(resultInt.get(0),resultInt.get(1)));
	    		}
	    	
	    	 return result.get(0).iterator();
	    	}
	   
	    }
		catch(Exception e) {
			e.printStackTrace();
			throw new DBAppException();
		}
	}

	private Vector<Record> intersect(Vector<Record> r1, Vector<Record> r2) {
	    Vector<Record> result = new Vector<Record>();
	   
//	    System.out.println(r2);
//	    for(Record r: r2) {
//	    	if( !(r.getV().get("gpa") instanceof Null) && (Double) r.getV().get("gpa")>1.8 &&)
//	    		System.out.println(r);
//	    }
	    if (r1 == null || r1.isEmpty() || r2 == null || r2.isEmpty()) {
	        return result;
	    }
	    for (Record r : r2) {
	    	
	        if (r1.contains(r)) {
//	        	System.out.println(r);
	            result.add(r);
//	            System.out.println(r2.contains(r));
	        }
	    }
//	    System.out.println(result);
//	    System.out.println(result.size());
//	    System.out.println(r1.size()+ "  " + r2.size());
	    return result;
	}


	private Vector<Record> union(Vector<Record> r1, Vector<Record> r2) {
	    Vector<Record> result = new Vector<Record>();
	    if (r1 != null) {
	        result.addAll(r1);
	    }
	    if (r2 != null) {
	        for (Record r : r2) {
	            if (!result.contains(r)) {
	                result.add(r);
	            }
	        }
	    }
	    return result;
	}


	private Vector<Record> symmetricDifference(Vector<Record> r1, Vector<Record> r2) {
	    Vector<Record> result = new Vector<Record>();
	    if (r1 == null || r1.isEmpty()) {
	        return r2;
	    }
	    if (r2 == null || r2.isEmpty()) {
	        return r1;
	    }
	    Vector<Record> intersection = intersect(r1, r2);
	    Vector<Record> union = union(r1, r2);
	    result.addAll(union);
	    result.removeAll(intersection);
	    return result;
	}



	private boolean checkIfTableExists(String tableName) throws DBAppException{
		BufferedReader br = null;
		FileReader fr = null;
		try {
			 fr = new FileReader("src/main/resources/metadata.csv");
			 br = new BufferedReader(fr);
			String s = br.readLine();
			while(s != null) {
				if(s.split(",")[0].equals(tableName))  {
					br.close();
				    return true;
				}
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
//							System.out.println(st);
//							System.out.println(htblColNameValue.get(s));
//							System.out.println(strTableName);
							typeTmam = htblColNameValue.get(s).toString().toLowerCase().compareTo(split[6].toLowerCase()) >=0 &&
									htblColNameValue.get(s).toString().toLowerCase().compareTo(split[7].toLowerCase()) <=0;
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
//							System.out.println(split[2]);
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
//             System.out.println("src/main/resources/data/" + tableName + table.getPages().get(i) + ".ser");
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
						e.printStackTrace();
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
	
	public static void main(String[] args) throws DBAppException, FileNotFoundException, IOException, ClassNotFoundException, ParseException {
		DBApp x = new DBApp();
		x.init();
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[3];
		arrSQLTerms[0] = new SQLTerm();
		arrSQLTerms[1] = new SQLTerm();
		arrSQLTerms[2] = new SQLTerm();
		arrSQLTerms[2]._strTableName = "students";
		arrSQLTerms[2]._strColumnName= "first_name";
		arrSQLTerms[2]._strOperator = "=";
		arrSQLTerms[2]._objValue = "WutyhM";
		arrSQLTerms[1]._strTableName = "students";
		arrSQLTerms[1]._strColumnName= "gpa";
		arrSQLTerms[1]._strOperator = ">";
		arrSQLTerms[1]._objValue = new Double( 1.8 );
		arrSQLTerms[0]._strTableName = "students";
		arrSQLTerms[0]._strColumnName= "gpa";
		arrSQLTerms[0]._strOperator = "<";
		arrSQLTerms[0]._objValue = new Double( 1.5 );
		String[]strarrOperators = new String[2];
		strarrOperators[0] = "OR";
		strarrOperators[1] = "OR";
//		 select * from Student where name = “John Noor” or gpa = 1.5;
		Iterator resultSet = x.selectFromTable(arrSQLTerms , strarrOperators);
		while(resultSet.hasNext()) {
			System.out.println(resultSet.next());
		}
		
//		
//	    // ---------------------1 Table creation, see whatsapp list and change values of htbl according to the test case------------------------
//	    Hashtable <String,String>  htblColNameType = new Hashtable <String,String> ( );
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
//		x.createTable("bla4", "id", htblColNameType, htblColNameMin, htblColNameMax);
//		
//		
//		// ---------------------2 Table insertion,deletion and update--------------------------
//		Hashtable <String,Object> htblColNameValue = new Hashtable( );
//		
////	    htblColNameValue.put("id", new Integer(15)); 
////	    htblColNameValue.put("hours",7.0); 
//	    htblColNameValue.put("id","43-0271"); 
////	    String dateString = "Tue May 14 00:00:00 EET 1901";
////	    SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
////	    Date date = dateFormat.parse(dateString);
////	    dateFormat.applyPattern("yyyy-MM-dd");
////	    String formattedDate = dateFormat.format(date);
////	    htblColNameValue.put("gpa", new Double(3)); 
//		
////	    x.insertIntoTable("students", htblColNameValue);
////	    x.updateTable("transcripts", "4.9996", htblColNameValue);
////	    x.deleteFromTable("courses", htblColNameValue);
//	    
		try {
			ObjectInputStream inp = new ObjectInputStream(new FileInputStream("src/main/resources/data/students.ser")); //hot hena esm el table el ayez pages beta3to
			Table b = (Table) inp.readObject();
			System.out.println(b.getTableName());
			System.out.println(b.getPages());
			System.out.println(b.getRows());
			Vector <Record>  v = x.deserializePage(b, 1);
			System.out.println(v);
//			System.out.println(v.size());
//			Vector <Record>  v2 = x.deserializePage(b, 2);
//			System.out.println(v2);
//			System.out.println(v2.size());
//			Vector <Record>  v3 = x.deserializePage(b, 3);
//			System.out.println(v3);
//			System.out.println(v3.size());
//			Vector <Record>  v4 = x.deserializePage(b, 4);
//			System.out.println(v4);
//			System.out.println(v4.size());
//			inp.close();
		}
		catch(Exception e)
		{}
	}

	
	
	

}
