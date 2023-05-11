package main.java;

//Helper to indicate if the there is an index on the cols
//search using the octree 
//1-insert, search 
//2-Update, if there is an index call update,
//3-delete, if there is an index , call delete(octree) delete in the OCTET ONLY (COMPARE WITH HTBL), this will return list of records to be deleted.
//4-select, search in octree if there is an index, filter the list according to the remaining conditions
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
import java.util.stream.Collectors;
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
			Record newRecord;
			if(table.getPages().size()==0){
				Vector <Record> newPage = new Vector <Record>();
				table.getPages().add(1);
				newRecord = new Record(htblColNameValue,1);
				newPage.add(newRecord);
				ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName +  "1.ser"));
				outputStream.writeObject(newPage);
				outputStream.close();
				
			}
			else {
				Vector <Record> currentPage = deserializePage(table, index[1]);
				newRecord = new Record(htblColNameValue,index[1]);
				currentPage.add(index[2],newRecord);
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
					newPage.get(0).page++;
					currentPage.remove(currentPage.size()-1);					
					ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + (index[1] +i) + ".ser"));
					ObjectOutputStream outputStream2 = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + (index[1]+ i -1) +  ".ser"));
					outputStream.writeObject(newPage);
					outputStream2.writeObject(currentPage);
					outputStream.close();
					outputStream2.close();
					serializeTable(table);
					currentPage = newPage;
					i++;
				}while( currentPage.size() > maxRowPerPage  );}
				else {
				ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + (table.getPages().lastElement()) +  ".ser"));
				outputStream.writeObject(currentPage);
				outputStream.close();}
			}
			table.setRows(table.getRows() +1);
			in.close();
			serializeTable(table);
			Hashtable <String,String> colIndexes =  StaticHelpers.checkIndex(strTableName);
			Set <String> cols = colIndexes.keySet();
			Set <String> indexes = new HashSet <String>();
			for(String s :cols) 
				indexes.add(colIndexes.get(s));
			for(String s : indexes) {
				Octree oct = Octree.deserialiazeOctree(s);
				oct.insert(newRecord);
			}
			System.gc();
			
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
			ArrayList <String> pkInfo = primaryKeyInfoAndCheck(strTableName, htblColNameValue,false);
			Object pk = parsePrimaryKey(pkInfo.get(0),strClusteringKeyValue);
			
			//3 Check existing primary Key, Search for the record using clusteringKey
			Hashtable <String,String> colIndexes =  StaticHelpers.checkIndex(strTableName);
			
			System.out.println(colIndexes);
			if(colIndexes.get(pkInfo.get(1)) != null) {
				System.out.println("IN HEREEE");
				Octree oct = Octree.deserialiazeOctree(colIndexes.get(pkInfo.get(1)));
//				oct.root.print(0);
				System.out.println(pkInfo.get(1));
				oct.update(pk, pk, pk, oct.root.boundsX[0].equals(pkInfo.get(1)) , oct.root.boundsY[0].equals(pkInfo.get(1)), oct.root.boundsZ[0].equals(pkInfo.get(1)) , htblColNameValue);
			}
			else {
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
			Record newie = new Record(htblColNameValue,oldValue.page);
			for(String s : oldie) {
				if(newie.getV().get(s)==null)
					newie.getV().put(s, oldValue.getV().get(s));
			}
			currentPage.add(index[0],new Record(htblColNameValue,oldValue.page));
			ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + index[1] + ".ser"));
			outputStream.writeObject(currentPage);
			outputStream.close();	
			in.close();
			}
		}
		catch(Exception e) {
						e.printStackTrace(); //StackTrace();
			throw new DBAppException(e.getMessage());
		}
		//5 worst case scenario  
	}
			
	public static Object parsePrimaryKey(String type, String strClusteringKeyValue) throws ParseException {
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
		boolean isThereAnIndex = false;
		checkDataTypes(strTableName, htblColNameValue);
		Hashtable <String,String> colIndexes = StaticHelpers.checkIndex(strTableName); 
		Set <String> cols = colIndexes.keySet();
		Set <String> indexes = new HashSet <String>();
		for(String s :cols) 
			indexes.add(colIndexes.get(s));
		for(String s : indexes) {
			Octree oct = Octree.deserialiazeOctree(s);
			isThereAnIndex = true;
			oct.delete(htblColNameValue.get(oct.root.boundsX[0]), htblColNameValue.get(oct.root.boundsY[0]), 
					htblColNameValue.get(oct.root.boundsZ[0]), htblColNameValue.keySet().contains(oct.root.boundsX[0]), 
					htblColNameValue.keySet().contains(oct.root.boundsY[0]), htblColNameValue.keySet().contains(oct.root.boundsZ[0]), htblColNameValue);
		}
		System.gc();
		if(isThereAnIndex)
			return;
		
		
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
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException  {
		//handle law sqlterm fady
		//bs for primary key
		try {
		checkIfTableExists(arrSQLTerms[0]._strTableName);
		//validate input
		Vector <Record> result = new Vector <Record>();
//	    result.add(new Vector <Record>());
		String strTableName = arrSQLTerms[0]._strTableName;
		
		ObjectInputStream in;
		in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + strTableName + ".ser"));
		Table table = (Table) in.readObject();
		ArrayList<String> pkInfo = primaryKeyInfoAndCheck(strTableName, null, false);
		Vector<Record> tempResult = null;
		boolean useIndex = true;
		String s = "";
		if(arrSQLTerms.length == 3 && strarrOperators.length == 2 && strarrOperators[0].equals("AND") && strarrOperators[1].equals("AND")) {
			Hashtable <String,String> colIndexes = StaticHelpers.checkIndex(strTableName);
			Set <String> cols = colIndexes.keySet();
			Set <String> index = new HashSet <String>(); // ask abt handle 6 cols on 2 indexes
//			for(String s :cols)
//				index.add(colIndexes.get(s));
			s = colIndexes.get(arrSQLTerms[0]);
			for(int i = 0;i<3;i++)
				if(!cols.contains(arrSQLTerms[i]._strColumnName) && !s.equals(colIndexes.get(arrSQLTerms[i]._strColumnName))) {
					useIndex = false;break;}
		}
		if(useIndex == true) {
			Octree oct = Octree.deserialiazeOctree(s);
			ArrayList <String> x = StaticHelpers.getMaxMinVals(strTableName, (String) oct.root.boundsX[0]);
			ArrayList <String> y = StaticHelpers.getMaxMinVals(strTableName, (String) oct.root.boundsY[0]);
			ArrayList <String> z = StaticHelpers.getMaxMinVals(strTableName, (String) oct.root.boundsZ[0]);
			int i,j,k=0;
			for(i = 0; i<3;i++)
				if(oct.root.boundsX[0].equals(arrSQLTerms[i]._strColumnName))
					break;
			for(j = 0; i<3;j++)
				if(oct.root.boundsX[0].equals(arrSQLTerms[j]._strColumnName))
					break;
			for(j = 0; i<3;k++)
				if(oct.root.boundsX[0].equals(arrSQLTerms[k]._strColumnName))
					break;
				
			ArrayList <Object> x2 = StaticHelpers.decipher(arrSQLTerms[i], x.get(0),x.get(1));
			ArrayList <Object> y2 = StaticHelpers.decipher(arrSQLTerms[j], y.get(0), y.get(1));
			ArrayList <Object> z2 = StaticHelpers.decipher(arrSQLTerms[k], z.get(0),z.get(1));
			
			List <Record> flattened = oct.root.search2(x2.get(2),y2.get(2),z2.get(2),x2.get(2),y2.get(2),z2.get(2)).stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
			return flattened.iterator();
			
			
			//return oct.search
		}
		
		
		for(int k=0;k<arrSQLTerms.length;k++) {
   		 SQLTerm current= arrSQLTerms[k];
   		 tempResult = new  Vector<Record>();
   		 if(arrSQLTerms[k]._strColumnName.equals(pkInfo.get(0))) {
   		 ArrayList<Object> decipher = StaticHelpers.decipher(arrSQLTerms[k],pkInfo.get(2), pkInfo.get(3));
   		 if((boolean)decipher.get(0))
   			 tempResult = StaticHelpers.binarySearchPK(decipher.get(2), decipher.get(3), (boolean)decipher.get(1), pkInfo, current._strTableName);
   			 break;
   		 }
   	 }
		if(tempResult == null) {
		for(int i0 = 0;i0<table.getPages().size();i0++) {
			Vector <Record> v = deserializePage(table, i0 +1);
			result = StaticHelpers.linearSelect(v, arrSQLTerms, strarrOperators);
	    }}
		else {
			result = StaticHelpers.linearSelect(tempResult, arrSQLTerms, strarrOperators);
		}
		 in.close();
	    return result.iterator();	 
	   
		}
		catch(Exception e) {
			e.printStackTrace();
			throw new DBAppException();
		}}
	
	public void createIndex(String strTableName,String[] strarrColName) throws DBAppException {
		//1-Tablename,colnames
		//get min,max vals of columns
		//Loop on all pages
		//insert in index if(null) throw exception
		//update metadatafile
		checkIfTableExists(strTableName);
		Object [] x = new Object [3];
		Object [] y = new Object [3];
		Object [] z = new Object [3];
		x[0] = strarrColName[0];
		y[0] = strarrColName[1];
		z[0] = strarrColName[2];
		ObjectInputStream in = null;
		//Feed x,y,z
		BufferedReader br = null;
		FileReader fr = null;
		try {
			 fr = new FileReader("src/main/resources/metadata.csv");
			 br = new BufferedReader(fr);
			String s = br.readLine();
			int i = 0;
			while(s != null) {
				String [] line = s.split(",");
				if(line[0].equals(strTableName) && line[1].equals(strarrColName[i]))  {
					if(i==0) {
						fr = new FileReader("src/main/resources/metadata.csv");
						 br = new BufferedReader(fr);
						x[1] = parsePrimaryKey(line[2], line[6]);x[2]=parsePrimaryKey(line[2], line[7]);}
					if(i==1) {
						fr = new FileReader("src/main/resources/metadata.csv");
						 br = new BufferedReader(fr);
						y[1] = parsePrimaryKey(line[2], line[6]); y[2]=parsePrimaryKey(line[2], line[7]);}
					if(i==2) {
						z[1] = parsePrimaryKey(line[2], line[6]);z[2]=parsePrimaryKey(line[2], line[7]);i++;break;}
					i++;
				}
				s = br.readLine();
			}
			
			br.close();
			fr.close();
//			System.out.println(i);
			if(i<3) throw new DBAppException("COL not found ya ebn el 3abeta");
		//Create the index
//			System.out.println(x[0]);
			Octree index = new Octree(strTableName,strarrColName[0]+strarrColName[1]+strarrColName[2], x, y, z);
			
			//Loop on pages,insert in index -> if (null) throw exception and delete index
			
			
			in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + strTableName + ".ser"));
			Table table = (Table) in.readObject();
			//no pk & no index
			for(i = 0;i<table.getPages().size();i++) {
				Vector <Record> v = deserializePage(table, i +1);
			    for (int j = 0; j < v.size(); j++) {
			    		checkNull(v.get(j));
			    		index.insert(v.get(j));
//			    		System.out.println(v.get(i));
			    	}
			    v = null;
			    System.gc();
		    }
			in.close();
			//, edit in metadata file
			StaticHelpers.editMetadata("src/main/resources/metadata.csv", strTableName, strarrColName, x, y, z);
//			index.root.print(0);
		}
		catch (Exception e) {
			e.printStackTrace();
			Octree.deleteIndex(strarrColName[0]+strarrColName[1]+strarrColName[2]);
			throw new DBAppException(e.getMessage());
		}
		
		
	}

	public void checkNull(Record r) throws DBAppException {
		Set <String> set = r.getV().keySet();
		for(String s: set)
			if(r.getV().get(s) instanceof Null)
				throw new DBAppException("NULL VALUES IN INDEX");
	}
	
	public static boolean performOperation(String operator, boolean bool1, boolean bool2) throws DBAppException {
	    switch (operator) {
	        case "AND":
	            return bool1 && bool2;
	        case "OR":
	            return bool1 || bool2;
	        case "XOR":
	            return bool1 ^ bool2;
	        default:
	            throw new DBAppException("Invalid operator: " + operator);
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
	
	//returns arraylist: datatype of pk,pk name,pk min, pk max
	private ArrayList <String> primaryKeyInfoAndCheck(String strTableName,Hashtable<String,Object> htblColNameValue,boolean insert) throws DBAppException {
		
		try {
			FileReader fr = new FileReader("src/main/resources/metadata.csv");
			BufferedReader br = new BufferedReader(fr);
			ArrayList <String> res = new ArrayList<String>(); 
			String s = br.readLine();
			String pk = "";
			String max="";
			String min="";
			while(s != null) {
				if(s.split(",")[0].equals(strTableName) && s.split(",")[3].equals("True")) {
					pk = s.split(",")[1]; //got the primary key
					res.add(s.split(",")[2]);
					min=s.split(",")[6];
					max=s.split(",")[7];
					
					break;
				}
				s = br.readLine();	
			}
			if( insert && (pk.equals("") || htblColNameValue.get(pk)==null)) {
				br.close();
				throw new DBAppException("noPk");
			}
			res.add(pk);
			br.close();
			res.add(min);
			res.add(max);
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
	
	
	
	public static Vector <Record> deserializePage (Table table, int pageNumber) throws FileNotFoundException, IOException, ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + table.getTableName() + pageNumber + ".ser"));
		Vector <Record> r =  (Vector <Record>) in.readObject();
		in.close();
		return r;
	}
	
	public static void main(String[] args) throws DBAppException, FileNotFoundException, IOException, ClassNotFoundException, ParseException {
		DBApp x = new DBApp();
		x.init();
		Hashtable <String,Object>  htblColNameValue = new Hashtable <String,Object> ( ); 
////	    htblColNameValue.put("id","43-0276"); 
	    htblColNameValue.put("gpa", new Double(3.8)); 
////	    htblColNameValue.put("dob", formattedDate);
	    htblColNameValue.put("first_name", "abdule");
//	    System.out.println(htblColNameValue);
        x.updateTable("students", "43-0276", htblColNameValue);
//        x.createIndex("students", new String []{"id","first_name","gpa"});
        
	   
//	    x.insertIntoTable("students", htblColNameValue);

//		x.createIndex("students", new String []{"gpa","dob","first_name"});
//		SQLTerm[] arrSQLTerms;
//		arrSQLTerms = new SQLTerm[3];
//		arrSQLTerms[0] = new SQLTerm();
//		arrSQLTerms[1] = new SQLTerm();
//		arrSQLTerms[2] = new SQLTerm();
//		arrSQLTerms[2]._strTableName = "students";
//		arrSQLTerms[2]._strColumnName= "first_name";
//		arrSQLTerms[2]._strOperator = "=";
//		arrSQLTerms[2]._objValue = "WutyhM";
//		arrSQLTerms[1]._strTableName = "students";
//		arrSQLTerms[1]._strColumnName= "gpa";
//		arrSQLTerms[1]._strOperator = "=";
//		arrSQLTerms[1]._objValue = new Double( 1.5 );
//		arrSQLTerms[0]._strTableName = "students";
//		arrSQLTerms[0]._strColumnName= "gpa";
//		arrSQLTerms[0]._strOperator = "=";
//		arrSQLTerms[0]._objValue = new Double( 1.69 );
//		String[]strarrOperators = new String[2];
//		strarrOperators[0] = "AND";
//		strarrOperators[1] = "AND";
////		 select * from Student where name = “John Noor” or gpa = 1.5;
//		Iterator resultSet = x.selectFromTable(arrSQLTerms , strarrOperators);
//		while(resultSet.hasNext()) {
//			System.out.println(resultSet.next());
//		}
//	    Octree o = Octree.deserialiazeOctree("idfirst_namegpa");
//	    o.update("43-0276", "43-0276", "43-0276", true, false, false, htblColNameValue);
		try {
			ObjectInputStream inp = new ObjectInputStream(new FileInputStream("src/main/resources/data/students.ser")); //hot hena esm el table el ayez pages beta3to
			Table b = (Table) inp.readObject();
			System.out.println(b.getTableName());
			System.out.println(b.getPages());
			System.out.println(b.getRows());
			Vector <Record>  v = x.deserializePage(b, 1);
			System.out.println(v);
			
	       
	        
//	        o.root.print(0);
//	        o.search();
			Octree y = Octree.deserialiazeOctree("idfirst_namegpa");
//			y.root.print(0);
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
