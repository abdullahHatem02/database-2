package main.java;
import java.io.BufferedReader;
import org.junit.jupiter.api.Assertions;
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

import main.java.Record;
public class DBApp {
	private int maxRowPerPage;
	public static int maxEntriesInNode;
	
	
	public void init() {
		try {
			Properties prop = new Properties();
			InputStream input = new FileInputStream("src/main/resources/DBApp.config") ;
			prop.load(input);
			maxRowPerPage = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
			maxEntriesInNode = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));;
//			System.out.println(maxEntriesInNode +"--------");
			File metadata = new File("src/main/resources/metadata.csv");
			metadata.createNewFile();
			
//			maxRowPerPage=5;
		}
		catch(Exception e) {
//			e.printStackTrace();
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
				csvWriter.write((strClusteringKeyColumn.equalsIgnoreCase(s)?"True":"False")+",");
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
		checkDataTypes(strTableName, htblColNameValue,false);
		
		
		int [] index = binarySearchMadeByUs(htblColNameValue.get(TypeAndPk.get(1)), TypeAndPk, strTableName);
		if(index[0] != -1)
			throw new DBAppException();
		ObjectInputStream in;
		
		
		
		try {
			Hashtable <String,String> colIndexes =  StaticHelpers.checkIndex(strTableName);
			Set <String> cols = colIndexes.keySet();
			for(String s :cols) 
				if(htblColNameValue.get(s) == null)
					throw new DBAppException("Cant insert null columns due to index");
			
			FileReader fr = new FileReader("src/main/resources/metadata.csv");
			BufferedReader br = new BufferedReader(fr);
			String st = br.readLine();
	
			while(st!=null) {
				String [] split = st.split(",");
				if(split[0].equalsIgnoreCase(strTableName)) {
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
//				System.out.println("Empty");
				Vector <Record> newPage = new Vector <Record>();
				table.getPages().add(1);
				newRecord = new Record(htblColNameValue,1,table.getRows()+1);
				newPage.add(newRecord);
				ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName +  "1.ser"));
				outputStream.writeObject(newPage);
				outputStream.close();
//				Hashtable <String,String> colIndexes =  StaticHelpers.checkIndex(strTableName);
//				Set <String> cols = colIndexes.keySet();
//				Set <String> indexes = new HashSet <String>();
//				for(String s :cols) 
//					indexes.add(colIndexes.get(s));
//				for(String s : indexes) {
//					Octree oct = Octree.deserialiazeOctree(s);
//					oct.insert(newRecord);
//				}
				System.gc();
			}
			else {
				Vector <Record> currentPage = deserializePage(table, index[1]);
				newRecord = new Record(htblColNameValue,index[1],table.getRows()+1);
				currentPage.add(index[2],newRecord);
//				System.out.println(currentPage +"currrr");
				
				
				int i =1;
				if(currentPage.size() > maxRowPerPage) {
//					System.out.println("max");
					Vector <Record> newPage;
//					System.out.println("page: " + index[1]);
					do {
					if(table.getPages().contains(index[1]+i)) {
//						System.out.println("exists");
						newPage = deserializePage(table, index[1]+i);
						
					}else {
					table.getPages().add(table.getPages().size()+1);
					newPage = new Vector <Record>();}
					
//					System.out.println("currrr: " +currentPage );
//					Hashtable <String,String> colIndexes =  StaticHelpers.checkIndex(strTableName);
//					Set <String> cols = colIndexes.keySet();
					Set <String> indexes = new HashSet <String>();
					int c = 0;
					for(String s :cols) 
						indexes.add(colIndexes.get(s));
					for(String s : indexes) {
						Octree oct = Octree.deserialiazeOctree(s);
//						System.out.println(oct.root.delete(null, null, null, false, false, false,newPage.get(0).getV()).size() + "size");
//						System.out.println();
//						System.out.println("will shift");
						Record r = currentPage.lastElement();
						oct.delete(null, null, null, false, false, false, r.getV()); 
						Node.serializePage(strTableName);
						Node.deserialized = new Hashtable<String, Vector < Record>>();
						if(c==0) {r.page++;}
//						System.out.println(currentPage.lastElement());
						oct.insert(currentPage.lastElement());
						
						Node.deserialized = new Hashtable<String, Vector < Record>>();
						c++;
					}
					if(c==0)currentPage.lastElement().page++;
//					System.out.println("Crrrr" + currentPage);
					
					newPage.add(0,currentPage.lastElement());
					
					currentPage.remove(currentPage.size()-1);
					
										
					ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + (index[1] +i) + ".ser"));
					ObjectOutputStream outputStream2 = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + (index[1]+ i -1) +  ".ser"));
					outputStream.writeObject(newPage);
					outputStream2.writeObject(currentPage);
					outputStream.close();
					outputStream2.close();
					serializeTable(table);
//					System.out.println(currentPage);
//					System.out.println(newPage);
//					System.out.println();
//					System.out.println(currentPage);
//					System.out.println(newPage);
					currentPage = newPage;
					
					i++;
				}while( currentPage.size() > maxRowPerPage  );}
				else {
					
					
				ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + index[1] +  ".ser"));
				
				outputStream.writeObject(currentPage);
				outputStream.close();}
			}
//			Hashtable <String,String> colIndexes =  StaticHelpers.checkIndex(strTableName);
//			Set <String> cols = colIndexes.keySet();
			Set <String> indexes = new HashSet <String>();
			for(String s :cols) 
				indexes.add(colIndexes.get(s));
			for(String s : indexes) {
				Octree oct = Octree.deserialiazeOctree(s);
				oct.insert(newRecord);
			}
			table.setRows(table.getRows() +1);
			in.close();
			serializeTable(table);
			
			
		} catch (Exception e) {
//			e.printStackTrace();
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
			checkDataTypes(strTableName, htblColNameValue,false);
			ArrayList <String> pkInfo = primaryKeyInfoAndCheck(strTableName, htblColNameValue,false);
			Object pk = parsePrimaryKey(pkInfo.get(0),strClusteringKeyValue);
			htblColNameValue.remove(pkInfo.get(1));
			//3 Check existing primary Key, Search for the record using clusteringKey
			Hashtable <String,String> colIndexes =  StaticHelpers.checkIndex(strTableName);
			
//			System.out.println(colIndexes);
			if(colIndexes.get(pkInfo.get(1)) != null) {
				Octree oct = Octree.deserialiazeOctree(colIndexes.get(pkInfo.get(1)));
				oct.update(pk, pk, pk, oct.root.boundsX[0].equals(pkInfo.get(1).toLowerCase()) , oct.root.boundsY[0].equals(pkInfo.get(1).toLowerCase()), oct.root.boundsZ[0].equals(pkInfo.get(1).toLowerCase()) , htblColNameValue);
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
			Record newie = new Record(htblColNameValue,oldValue.page,table.getRows()+1);
			table.setRows(table.getRows()+1);
			for(String s : oldie) {
				if(newie.getV().get(s)==null)
					newie.getV().put(s, oldValue.getV().get(s));
			}
			currentPage.add(index[0],new Record(htblColNameValue,oldValue.page,oldValue.id));
//			Hashtable <String,String> colIndexes =  StaticHelpers.checkIndex(strTableName);
			Set <String> cols = colIndexes.keySet();
			Set <String> indexes = new HashSet <String>();
			for(String s :cols) 
				indexes.add(colIndexes.get(s));
			for(String s : indexes) {
				Octree oct = Octree.deserialiazeOctree(s);
				oct.delete(null,null, null, false, false, false, oldValue.getV());
				Node.deserialized = new Hashtable<String, Vector < Record>>();
				oct.insert(currentPage.get(index[0]));
				Node.deserialized = new Hashtable<String, Vector < Record>>();
			}
			
			ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + strTableName + index[1] + ".ser"));
			outputStream.writeObject(currentPage);
			outputStream.close();	
			in.close();
			}
		}
		catch(Exception e) {
//						e.printStackTrace(); //StackTrace();
			throw new DBAppException(e.getMessage());
		}
		//5 worst case scenario  
	}
			
	public static Object parsePrimaryKey(String type, String strClusteringKeyValue) throws ParseException {
		Object res = strClusteringKeyValue;
		type = type.substring(10);
		if(type.equalsIgnoreCase("Integer")) {
			res = Integer.parseInt(strClusteringKeyValue);
		}
		else if(type.equalsIgnoreCase("Double")) {
			res = Double.parseDouble(strClusteringKeyValue);
					}
		else if(type.equalsIgnoreCase("Date")) {
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
		checkDataTypes(strTableName, htblColNameValue,true);
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
//			Node.deserialized = new Hashtable<String, Vector<Record>>();
		}
		
		
		System.gc();
		if(isThereAnIndex) {
			Node.serializePage(strTableName);
//			System.out.println("Done");
			return;}
		
		
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
			if(index[0] == -1) {
				return;
			}
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
//			e.printStackTrace();
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
		for(int i = 0;i<arrSQLTerms.length-1;i++)
			if(!arrSQLTerms[i]._strTableName.equalsIgnoreCase(arrSQLTerms[i+1]._strTableName))
				throw new DBAppException("No joins");
		
		boolean useIndex = false;
		String s = "";
		if(arrSQLTerms.length == 3 && strarrOperators.length == 2 && strarrOperators[0].equalsIgnoreCase("AND") && strarrOperators[1].equals("AND")) {
			Hashtable <String,String> colIndexes = StaticHelpers.checkIndex(strTableName);
//			System.out.println(colIndexes);
			Set <String> cols = colIndexes.keySet();
			Set <String> index = new HashSet <String>(); // ask abt handle 6 cols on 2 indexes
//			for(String s :cols)
//				index.add(colIndexes.get(s));
			s = colIndexes.get(arrSQLTerms[0]._strColumnName);
			
			if(s!=null) {
			for(int i = 0;i<3;i++) {
				if(!cols.contains(arrSQLTerms[i]._strColumnName) && !s.equalsIgnoreCase(colIndexes.get(arrSQLTerms[i]._strColumnName)) || arrSQLTerms[i]._strOperator.equals("!=")) {
//					System.out.println("breaking");
					break;}
				else if(i==2)
					useIndex = true;
				}
			
			}
		}
		else
			useIndex = false;
		if(useIndex == true) {
//			System.out.println("here");
			Octree oct = Octree.deserialiazeOctree(s);
			ArrayList <String> x = StaticHelpers.getMaxMinVals(strTableName, (String) oct.root.boundsX[0]);
			ArrayList <String> y = StaticHelpers.getMaxMinVals(strTableName, (String) oct.root.boundsY[0]);
			ArrayList <String> z = StaticHelpers.getMaxMinVals(strTableName, (String) oct.root.boundsZ[0]);
		
			int i,j,k=0;
			for(i = 0; i<3;i++)
				if(oct.root.boundsX[0].equals(arrSQLTerms[i]._strColumnName.toLowerCase()))
					break;
			for(j = 0; j<3;j++)
				if(oct.root.boundsY[0].equals(arrSQLTerms[j]._strColumnName.toLowerCase()))
					break;
			for(k = 0; k<3;k++)
				if(oct.root.boundsZ[0].equals(arrSQLTerms[k]._strColumnName.toLowerCase()))
					break;
			Hashtable <String,String> datatypes =StaticHelpers.getDataTypes(strTableName);
			
			
			
			ArrayList <Object> x2 = StaticHelpers.decipher(arrSQLTerms[i], x.get(0),x.get(1),datatypes.get(oct.root.boundsX[0]));
			ArrayList <Object> y2 = StaticHelpers.decipher(arrSQLTerms[j], y.get(0), y.get(1),datatypes.get(oct.root.boundsY[0]));
			ArrayList <Object> z2 = StaticHelpers.decipher(arrSQLTerms[k], z.get(0),z.get(1),datatypes.get(oct.root.boundsZ[0]));
//			System.out.println(x2);
//			System.out.println(y2);
//			System.out.println(z2);
			
			List <Record> flattened = oct.root.search2(x2.get(2),x2.get(3),y2.get(2),y2.get(3),z2.get(2),z2.get(3)).stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
			return flattened.iterator();
		}
		
		boolean canBs = true;
		for(int k = 0;k<strarrOperators.length;k++) {
			if(!strarrOperators[k].equalsIgnoreCase("AND")) {
				canBs = false;break;}
		}
		if(canBs) {
		for(int k=0;k<arrSQLTerms.length;k++) {
   		 SQLTerm current= arrSQLTerms[k];
   		 
   		 if(arrSQLTerms[k]._strColumnName.equals(pkInfo.get(1).toLowerCase())) {
   		
   		 ArrayList<Object> decipher = StaticHelpers.decipher(arrSQLTerms[k],pkInfo.get(2), pkInfo.get(3),pkInfo.get(0));
   		 if((boolean)decipher.get(0)) {
   			tempResult = new  Vector<Record>();
   			 tempResult = StaticHelpers.binarySearchPK(decipher.get(2), decipher.get(3), (boolean)decipher.get(1), pkInfo, current._strTableName);
//   			 System.out.println(tempResult + "tempo");
   			 break;}
   		 }
   	 }}
		
		if(tempResult == null || tempResult.size() == 0) {
			
		for(int i0 = 0;i0<table.getPages().size();i0++) {
			Vector <Record> v = deserializePage(table, i0 +1);
			result.addAll( StaticHelpers.linearSelect(v, arrSQLTerms, strarrOperators));
//			result =  StaticHelpers.linearSelect(v, arrSQLTerms, strarrOperators);
	    }}
		else {
			
			result = StaticHelpers.linearSelect(tempResult, arrSQLTerms, strarrOperators);
		}
		 in.close();
	    return result.iterator();	 
	   
		}
		catch(Exception e) {
//			e.printStackTrace();
			throw new DBAppException();
		}}
	
	public void createIndex(String strTableName,String[] strarrColName) throws DBAppException {
		//1-Tablename,colnames
		//get min,max vals of columns
		//Loop on all pages
		//insert in index if(null) throw exception
		//update metadatafile
		checkIfTableExists(strTableName);
		if(strarrColName.length <3)
			throw new DBAppException("Less than 3 cols");
		Object [] x = new Object [3];
		Object [] y = new Object [3];
		Object [] z = new Object [3];
		x[0] = strarrColName[0].toLowerCase();
		y[0] = strarrColName[1].toLowerCase();
		z[0] = strarrColName[2].toLowerCase();
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
				if(line[0].equalsIgnoreCase(strTableName) && line[1].equalsIgnoreCase(strarrColName[i]))  {
//					System.out.println(line[5]);
					if(line[5].equalsIgnoreCase("Octree"))
						throw new DBAppException("Index already on this column");
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
			if(i<3) throw new DBAppException("<3 kalboba cols");
		//Create the index
			Octree index = new Octree(strTableName.toLowerCase(),strarrColName[0]+strarrColName[1]+strarrColName[2], x, y, z);
			//Loop on pages,insert in index -> if (null) throw exception and delete index
			in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + strTableName + ".ser"));
			Table table = (Table) in.readObject();
			//no pk & no index
			for(i = 0;i<table.getPages().size();i++) {
				Vector <Record> v = deserializePage(table, i +1);
			    for (int j = 0; j < v.size(); j++) {
			    		checkNull(v.get(j),strarrColName);
			    		index.insert(v.get(j));
//			    		System.out.println(v.get(i));
			    	}
			    v = null;
			    System.gc();
		    }
			in.close();
			//, edit in metadata file
//			System.out.println("here");
			StaticHelpers.editMetadata("src/main/resources/metadata.csv", strTableName, strarrColName, x, y, z);
//			index.root.print(0);
		}
		catch (Exception e) {
			try {
				br.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
//				e1.printStackTrace();
			}
//			e.printStackTrace();
//			Octree.deleteIndex(strarrColName[0]+strarrColName[1]+strarrColName[2]);
			throw new DBAppException(e.getMessage());
			
		}
		
		
	}

	public void checkNull(Record r,String cols []) throws DBAppException {
//		Set <String>  = r.getV().keySet();
		for(String s: cols)
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
				if(s.split(",")[0].equalsIgnoreCase(tableName))  {
					br.close();
				    return true;
				}
				s = br.readLine();
			}
			br.close();
			fr.close();
			return false;
		} catch (Exception e) {
			e.printStackTrace();
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
			if(x.equalsIgnoreCase(clusteringKey))
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
//				System.out.println(s);
				if(s.split(",")[0].equalsIgnoreCase(strTableName) && s.split(",")[3].equalsIgnoreCase("True")) {
					
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
//			e.printStackTrace();
			throw new DBAppException(e.getMessage());
		}
	}
	
	private boolean checkDataTypes(String strTableName,Hashtable<String,Object> htblColNameValue,boolean delete) throws DBAppException{
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
					if(split[1].equalsIgnoreCase(s) && split[0].equalsIgnoreCase(strTableName)) {
						flagAfashnaElColumn = true;
						
						if(!split[2].equalsIgnoreCase(htblColNameValue.get(s).getClass().toString().substring(6))) {
							br.close();
							
//							System.out.println(htblColNameValue.get(s).getClass().toString().substring(6));
				         	throw new DBAppException("wrong datatype");
						}
						String type = split[2].substring(10);
						boolean typeTmam =true;
						if(!delete) {
						if(type.equalsIgnoreCase("Integer")) {
							
							typeTmam = Integer.parseInt(htblColNameValue.get(s).toString()) >= Integer.parseInt(split[6]) &&
									Integer.parseInt(htblColNameValue.get(s).toString()) <= Integer.parseInt(split[7]);
						}
						else if(type.equalsIgnoreCase("String")) {
//							System.out.println(st);
//							System.out.println(htblColNameValue.get(s));
//							System.out.println(strTableName);
							typeTmam = htblColNameValue.get(s).toString().toLowerCase().compareTo(split[6].toLowerCase()) >=0 &&
									htblColNameValue.get(s).toString().toLowerCase().compareTo(split[7].toLowerCase()) <=0;
						}
						else if(type.equalsIgnoreCase("Double")) {
							typeTmam = Double.parseDouble(htblColNameValue.get(s).toString()) >= Double.parseDouble(split[6]) &&
									Double.parseDouble(htblColNameValue.get(s).toString()) <= Double.parseDouble(split[7]);
						}
						else if(type.equalsIgnoreCase("Date")) {
							SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//							System.out.println(htblColNameValue.get(cols));
							typeTmam = ((Date )  htblColNameValue.get(s)).compareTo(dateFormat.parse(split[6])) >= 0 &&
									 ((Date )  htblColNameValue.get(s)).compareTo(dateFormat.parse(split[7])) <= 0;
							
						}
						if(!typeTmam) {
//							System.out.println(split[2]);
							br.close();
							throw new DBAppException("error");
							}
					}}
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
//			e.printStackTrace()
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
//        System.out.println(table.getPages().size());
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
//						e.printStackTrace();
		 throw new DBAppException("bs");
		}
	}

	static void serializeTable (Table table) throws FileNotFoundException, IOException {
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
	
	
	public Iterator parseSQL(StringBuffer strbufSQL) throws DBAppException {
	    String sql = strbufSQL.toString().trim();
	    String[] tokens = sql.split("\\s+");
	    String command = tokens[0].toUpperCase();
	    String quotes = "'";
	    String doubleQuotes = "\"";
	    for(int i = 1; i < tokens.length; i++) {
	    	tokens[i] = tokens[i].toUpperCase();
	    	if(tokens[i].contains(",") || tokens[i].contains("(") || tokens[i].contains(")") || tokens[i].contains(quotes) || tokens[i].contains(doubleQuotes)) {
	    		tokens[i] = tokens[i].replace(",","");
	    		tokens[i] = tokens[i].replace("(","");
	    		tokens[i] = tokens[i].replace(")","");
	    		tokens[i] = tokens[i].replace(quotes,"");
	    		tokens[i] = tokens[i].replace(doubleQuotes,"");
	    	}
	    	System.out.println(tokens[i]);
	    }
	    
	    switch(command){
	    case "CREATE": if(tokens[1].equalsIgnoreCase("TABLE")) {BonusHelpers.createTableParser(tokens,this);}else if (command.equalsIgnoreCase("INDEX")){BonusHelpers.createIndexParser(tokens,this);} else{ throw new DBAppException();};break;
	    case "SELECT": return BonusHelpers.selectParser(tokens,this);
	    case "INSERT": BonusHelpers.insertParser(tokens,this);break;
	    case "DELETE": BonusHelpers.deleteParser(tokens,this);break;
	    case "UPDATE": BonusHelpers.updateParser(tokens,this);break;
	    default: throw new DBAppException("Unsupported");
	    }
	     return null;
	}
	
	
	 private static void  insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
	        BufferedReader coursesTable = new BufferedReader(new FileReader("src/main/resources/courses_table.csv"));
	        String record;
	        Hashtable<String, Object> row = new Hashtable<>();
	        int c = limit;
	        if (limit == -1) {
	            c = 1;
	        }
	        while ((record = coursesTable.readLine()) != null && c > 0) {
	            String[] fields = record.split(",");


	            int year = Integer.parseInt(fields[0].trim().substring(0, 4));
	            int month = Integer.parseInt(fields[0].trim().substring(5, 7));
	            int day = Integer.parseInt(fields[0].trim().substring(8));

	            Date dateAdded = new Date(year - 1900, month - 1, day);

	            row.put("date_added", dateAdded);

	            row.put("course_id", fields[1]);
	            row.put("course_name", fields[2]);
	            row.put("hours", Integer.parseInt(fields[3]));

	            dbApp.insertIntoTable("courses", row);
	            row.clear();

	            if (limit != -1) {
	                c--;
	            }
	        }

	        coursesTable.close();
	    }
	
	 private static void  insertStudentRecords(DBApp dbApp, int limit) throws Exception {
	        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
	        String record;
	        int c = limit;
	        if (limit == -1) {
	            c = 1;
	        }

	        Hashtable<String, Object> row = new Hashtable<>();
	        while ((record = studentsTable.readLine()) != null && c > 0) {
	            String[] fields = record.split(",");

	            row.put("id", fields[0]);
	            row.put("first_name", fields[1]);
	            row.put("last_name", fields[2]);

	            int year = Integer.parseInt(fields[3].trim().substring(0, 4));
	            int month = Integer.parseInt(fields[3].trim().substring(5, 7));
	            int day = Integer.parseInt(fields[3].trim().substring(8));

	            Date dob = new Date(year - 1900, month - 1, day);
	            row.put("dob", dob);

	            double gpa = Double.parseDouble(fields[4].trim());

	            row.put("gpa", gpa);

	            dbApp.insertIntoTable("students", row);
	            row.clear();
	            if (limit != -1) {
	                c--;
	            }
	        }
	        studentsTable.close();
	    }
	 private static void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
	        BufferedReader transcriptsTable = new BufferedReader(new FileReader("src/main/resources/transcripts_table.csv"));
	        String record;
	        Hashtable<String, Object> row = new Hashtable<>();
	        int c = limit;
	        if (limit == -1) {
	            c = 1;
	        }
	        while ((record = transcriptsTable.readLine()) != null && c > 0) {
	            String[] fields = record.split(",");

	            row.put("gpa", Double.parseDouble(fields[0].trim()));
	            row.put("student_id", fields[1].trim());
	            row.put("course_name", fields[2].trim());

	            String date = fields[3].trim();
	            int year = Integer.parseInt(date.substring(0, 4));
	            int month = Integer.parseInt(date.substring(5, 7));
	            int day = Integer.parseInt(date.substring(8));

	            Date dateUsed = new Date(year - 1900, month - 1, day);
	            row.put("date_passed", dateUsed);

	            dbApp.insertIntoTable("transcripts", row);
	            row.clear();

	            if (limit != -1) {
	                c--;
	            }
	        }

	        transcriptsTable.close();
	    }
	 private static void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
	        BufferedReader pcsTable = new BufferedReader(new FileReader("src/main/resources/pcs_table.csv"));
	        String record;
	        Hashtable<String, Object> row = new Hashtable<>();
	        int c = limit;
	        if (limit == -1) {
	            c = 1;
	        }
	        while ((record = pcsTable.readLine()) != null && c > 0) {
	            String[] fields = record.split(",");

	            row.put("pc_id", Integer.parseInt(fields[0].trim()));
	            row.put("student_id", fields[1].trim());

	            dbApp.insertIntoTable("pcs", row);
	            row.clear();

	            if (limit != -1) {
	                c--;
	            }
	        }

	        pcsTable.close();
	    }
	 private static void createTranscriptsTable(DBApp dbApp) throws Exception {
	        // Double CK
	        String tableName = "transcripts";

	        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
	        htblColNameType.put("gpa", "java.lang.Double");
	        htblColNameType.put("student_id", "java.lang.String");
	        htblColNameType.put("course_name", "java.lang.String");
	        htblColNameType.put("date_passed", "java.util.Date");

	        Hashtable<String, String> minValues = new Hashtable<>();
	        minValues.put("gpa", "0.7");
	        minValues.put("student_id", "43-0000");
	        minValues.put("course_name", "AAAAAA");
	        minValues.put("date_passed", "1990-01-01");

	        Hashtable<String, String> maxValues = new Hashtable<>();
	        maxValues.put("gpa", "5.0");
	        maxValues.put("student_id", "99-9999");
	        maxValues.put("course_name", "zzzzzz");
	        maxValues.put("date_passed", "2020-12-31");

	        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
	    }

	    private static void createStudentTable(DBApp dbApp) throws Exception {
	        // String CK
	        String tableName = "students";

	        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
	        htblColNameType.put("id", "java.lang.String");
	        htblColNameType.put("first_name", "java.lang.String");
	        htblColNameType.put("last_name", "java.lang.String");
	        htblColNameType.put("dob", "java.util.Date");
	        htblColNameType.put("gpa", "java.lang.Double");

	        Hashtable<String, String> minValues = new Hashtable<>();
	        minValues.put("id", "43-0000");
	        minValues.put("first_name", "AAAAAA");
	        minValues.put("last_name", "AAAAAA");
	        minValues.put("dob", "1990-01-01");
	        minValues.put("gpa", "0.7");

	        Hashtable<String, String> maxValues = new Hashtable<>();
	        maxValues.put("id", "99-9999");
	        maxValues.put("first_name", "zzzzzz");
	        maxValues.put("last_name", "zzzzzz");
	        maxValues.put("dob", "2000-12-31");
	        maxValues.put("gpa", "5.0");

	        dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
	    }
	    private static void createPCsTable(DBApp dbApp) throws Exception {
	        // Integer CK
	        String tableName = "pcs";

	        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
	        htblColNameType.put("pc_id", "java.lang.Integer");
	        htblColNameType.put("student_id", "java.lang.String");


	        Hashtable<String, String> minValues = new Hashtable<>();
	        minValues.put("pc_id", "0");
	        minValues.put("student_id", "43-0000");

	        Hashtable<String, String> maxValues = new Hashtable<>();
	        maxValues.put("pc_id", "20000");
	        maxValues.put("student_id", "99-9999");

	        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
	    }
	    private static void createCoursesTable(DBApp dbApp) throws Exception {
	        // Date CK
	        String tableName = "courses";

	        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
	        htblColNameType.put("date_added", "java.util.Date");
	        htblColNameType.put("course_id", "java.lang.String");
	        htblColNameType.put("course_name", "java.lang.String");
	        htblColNameType.put("hours", "java.lang.Integer");


	        Hashtable<String, String> minValues = new Hashtable<>();
	        minValues.put("date_added", "1901-01-01");
	        minValues.put("course_id", "0000");
	        minValues.put("course_name", "AAAAAA");
	        minValues.put("hours", "1");

	        Hashtable<String, String> maxValues = new Hashtable<>();
	        maxValues.put("date_added", "2020-12-31");
	        maxValues.put("course_id", "9999");
	        maxValues.put("course_name", "zzzzzz");
	        maxValues.put("hours", "24");

	        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

	    }
	    public void testWrongStudentsKeyInsertion() {
	        final DBApp dbApp = new DBApp();
	        dbApp.init();

	        String table = "students";
	        Hashtable<String, Object> row = new Hashtable();
	        row.put("id", 123);
	        
	        row.put("first_name", "foo");
	        row.put("last_name", "bar");

	        Date dob = new Date(1995 - 1900, 4 - 1, 1);
	        row.put("dob", dob);
	        row.put("gpa", 1.1);

	        Assertions.assertThrows(DBAppException.class, () -> {
	                    dbApp.insertIntoTable(table, row);
	                }
	        );

	    }
	    public void testExtraTranscriptsInsertion() {
	        final DBApp dbApp = new DBApp();
	        dbApp.init();

	        String table = "transcripts";
	        Hashtable<String, Object> row = new Hashtable();
	        row.put("gpa", 1.5);
	        row.put("student_id", "34-9874");
	        row.put("course_name", "bar");
	        row.put("elective", true);


	        Date date_passed = new Date(2011 - 1900, 4 - 1, 1);
	        row.put("date_passed", date_passed);


	        Assertions.assertThrows(DBAppException.class, () -> {
	                    dbApp.insertIntoTable(table, row);
	                }
	        );
	    }
	  
	  public static void main(String[] args) throws Exception {
	      DBApp db = new DBApp();
	      db.init();
	      
	  
		StringBuffer create = new StringBuffer("create table employee (id INT check (id > 1 AND id < 9), name VARCHAR(255) check (name > a AND name < z), count int check (count > 0 AND count < 9999), PRIMARY KEY (id))");	
		StringBuffer select = new StringBuffer("select * from EMPLOYEE where id <= 9 or name = 'Hassan'");
//		StringBuffer insert = new StringBuffer("insert into employee (id, name, count) values (10, 'NOURA', 5)");
		StringBuffer index = new StringBuffer("CREATE INDEX hijk ON hijk (id, count, name)");
		StringBuffer delete = new StringBuffer("DELETE FROM employee WHERE name = 'ALI' ");
		StringBuffer update = new StringBuffer("UPDATE employee SET name = 'Ali', count = 4 WHERE id = 7");
//		db.parseSQL(create);
//		db.parseSQL(insert);
		Iterator res =  db.parseSQL(select);
		System.out.println("resultlt");
		while(res.hasNext()) {
			System.out.println(res.next());
		}
//		db.parseSQL(update);
//		db.parseSQL(delete);
//		db.parseSQL(index);
	      
//	      db.createIndex("students", new String [] {"first_name","last_name","gpa"});

//	        SQLTerm[] arrSQLTerms;
//	        arrSQLTerms = new SQLTerm[2];
//	        arrSQLTerms[0] = new SQLTerm();
//	        arrSQLTerms[0]._strTableName = "students";
//	        arrSQLTerms[0]._strColumnName= "first_name";
//	        arrSQLTerms[0]._strOperator = "=";
//	        arrSQLTerms[0]._objValue =row.get("first_name");
//
//	        arrSQLTerms[1] = new SQLTerm();
//	        arrSQLTerms[1]._strTableName = "students";
//	        arrSQLTerms[1]._strColumnName= "gpa";
//	        arrSQLTerms[1]._strOperator = "<=";
//	        arrSQLTerms[1]._objValue = row.get("gpa");
//
//	        String[]strarrOperators = new String[1];
//	        strarrOperators[0] = "OR";
//	      String table = "students";w
//	        row.put("first_name", "fooooo");
//	        row.put("last_name", "baaaar");

//	        Date dob = new Date(1992 - 1900, 9 - 1, 8);
//	        row.put("dob", dob);
//	        row.put("gpa", 1.1);

//	        db.updateTable(table, "47-2286", row);
//	      createCoursesTable(db);
//	      createPCsTable(db);
//	      createTranscriptsTable(db);
//	      createStudentTable(db);
//	      insertPCsRecords(db,200);
//	      insertTranscriptsRecords(db,200);
//	      insertStudentRecords(db,200);
//	      insertCoursesRecords(db,200);
//	        String table = "students";
//	        Hashtable<String, Object> row = new Hashtable();
	        
//	        SQLTerm[] arrSQLTerms;
//			arrSQLTerms = new SQLTerm[3];
//			arrSQLTerms[0] = new SQLTerm();
//			arrSQLTerms[1] = new SQLTerm();
//			arrSQLTerms[2] = new SQLTerm();
//			
//			arrSQLTerms[0]._strTableName = "students";
//			arrSQLTerms[0]._strColumnName= "gpa";
//			arrSQLTerms[0]._strOperator = "<=";
//			arrSQLTerms[0]._objValue = 3.2;
//
//			arrSQLTerms[1]._strTableName = "students";
//			arrSQLTerms[1]._strColumnName= "first_name";
//			arrSQLTerms[1]._strOperator = "<";
//			arrSQLTerms[1]._objValue = "nouraa";
//			
//			arrSQLTerms[2]._strTableName = "students";
//			arrSQLTerms[2]._strColumnName= "last_name";
//			arrSQLTerms[2]._strOperator = "<";
//			arrSQLTerms[2]._objValue = "gggggg";
//			String[]strarrOperators = new String[2];
//			strarrOperators[0] = "AND";
//			strarrOperators[1] = "AND";
//			
//
//			Iterator resultSet = db.selectFromTable(arrSQLTerms , strarrOperators);
//			
//			while(resultSet.hasNext()) {
//				System.out.println(resultSet.next());
//			}
//	        row.put("id", 123);
//	        
//	        row.put("id", "47-2302");
//	        row.put("last_name", "nouraa");
//	        row.put("first_name", "sadekk");
//
//	        Date dob = new Date(1995 - 1900, 4 - 1, 1);
//	        row.put("dob", dob);
//	        row.put("gpa", 1.25);
//	        row.put("os", 1.1);
	      
	        //47-2285
	        
//	      db.insertIntoTable(table, row);
//	        db.updateTable(table, "47-2302", row);
//	        db.deleteFromTable(table, row);
	      
//	      String table = "transcripts";
//	        Hashtable<String, Object> row = new Hashtable();
//	        row.put("gpa", 1.5);
//	        row.put("student_id", "44-9874");
	      //  row.put("course_name", "bar");
	      //  row.put("elective", true);


//	        Date date_passed = new Date(2011 - 1900, 4 - 1, 1);
//	        row.put("date_passed", date_passed);
//	        db.insertIntoTable(table, row);
	        
	        ObjectInputStream in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + "EMPLOYEE" + ".ser"));
			Table des = (Table) in.readObject();
	        Vector <Record> page = deserializePage(des, 1);
	        
	        
//	        System.out.println(page);
	      
//		  Octree x = Octree.deserialiazeOctree("first_namelast_namegpa");
//		  x.root.print(0);
	  }
}
	

//	public static void main(String[] args) throws DBAppException {
//		DBApp x = new DBApp();
//		x.init();
//		StringBuffer create = new StringBuffer("create tablee hijkk (id INT check (id > 1 AND id < 9), name VARCHAR(255) check (name > a AND name < z), count int check (count > 0 AND count < 9999), PRIMARY KEY (id))");	
//		StringBuffer select = new StringBuffer("select * from hijk where id >= 7 or name = 'Hassan' or count = 5");
//		StringBuffer insert = new StringBuffer("insert into hijk (id, name, count) values (7, 'Hassan', 5)");
//		StringBuffer index = new StringBuffer("CREATE INDEX hijk ON hijk (id, count, name)");
//		StringBuffer delete = new StringBuffer("DELETE FROM hijk WHERE id = 7 name = 'Hassan' AND count = 5");
//		StringBuffer update = new StringBuffer("UPDATE hijk SET name = 'Hassouna', count = 5 WHERE id = 7");
//		x.parseSQL(create);
//		x.parseSQL(insert);
//		x.parseSQL(select);
//		x.parseSQL(update);
//		x.parseSQL(delete);
//		x.parseSQL(index);
//	}
//	public static void main(String[] args) throws Exception {
//		DBApp x = new DBApp();
//		x.init();
////		x.createIndex("students", new String []{"last_name","first_name","gpa"});
//		SQLTerm[] arrSQLTerms;
//		arrSQLTerms = new SQLTerm[3];
//		arrSQLTerms[0] = new SQLTerm();
//		arrSQLTerms[1] = new SQLTerm();
//		arrSQLTerms[2] = new SQLTerm();
//		
//		arrSQLTerms[2]._strTableName = "students";
//		arrSQLTerms[2]._strColumnName= "first_name";
//		arrSQLTerms[2]._strOperator = "<=";
//		arrSQLTerms[2]._objValue = "xxxxxx";
//
//		arrSQLTerms[1]._strTableName = "students";
//		arrSQLTerms[1]._strColumnName= "last_name";
//		arrSQLTerms[1]._strOperator = "<=";
//		arrSQLTerms[1]._objValue = "xxxxxx";
//		
//		arrSQLTerms[0]._strTableName = "students";
//		arrSQLTerms[0]._strColumnName= "gpa";
//		arrSQLTerms[0]._strOperator = ">";
//		arrSQLTerms[0]._objValue = 1.0;
//		String[]strarrOperators = new String[2];
//		strarrOperators[0] = "AND";
//		strarrOperators[1] = "AND";
//		
//		long before = System.nanoTime();
//		Iterator resultSet = x.selectFromTable(arrSQLTerms , strarrOperators);
//		long after = System.nanoTime();
//		System.out.println(after-before);
//		while(resultSet.hasNext()) {
//			System.out.println(resultSet.next());
//		}
////		
//		
//		//110385500
//		//79005900
//		//4085807201
//		//2103768101
//	}
	

	
	
	


