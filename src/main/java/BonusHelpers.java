package main.java;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Hashtable;

public class BonusHelpers {
	public static void selectParser(String[] tokens,DBApp x) {
		
	}
	
	
	public static void insertParser(String[] tokens, DBApp x) {
		String strTableName = tokens[2];
		Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>();
		Hashtable<String,String> dataTypes = new Hashtable<String,String>();
		try {
			dataTypes = StaticHelpers.getDataTypes(strTableName);
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String val = "";
		boolean valuesCheck = false;
		ArrayList<String> colNames = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		ArrayList<Object> valuesObj = new ArrayList<Object>();
		for(int i = 3; i < tokens.length; i++) {
			if(tokens[i].equals("VALUES")) {
				valuesCheck = true;
				continue;
			}
			if(valuesCheck) {
				values.add(tokens[i]);
				System.out.println(tokens[i] + " values");
			}
			else {
				colNames.add(tokens[i]);
				System.out.println(tokens[i] + " colNames");
			}
		
		}
		for(int j = 0; j < colNames.size(); j++) {
//			System.out.println(colNames.get(j) + "-----------");
			val = dataTypes.get(colNames.get(j));
//			System.out.println(val);
			switch(val){
			case "java.lang.Integer": valuesObj.add(Integer.parseInt(values.get(j)));continue;
			case "java.lang.String": valuesObj.add(values.get(j));continue;	
			case "java.lang.Double": valuesObj.add(Float.parseFloat(values.get(j)));continue;
			case "java.lang.Date": SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			try {
				valuesObj.add(dateFormat.parse(values.get(j)));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}continue;
			}
		}
			for(int k = 0; k < colNames.size(); k++) {
				
				htblColNameValue.put(colNames.get(k), valuesObj.get(k));
				System.out.println("colNames: "+ colNames.get(k) + " values:"+ valuesObj.get(k));
			}
			
			System.out.println("Value of A: " + val);
		
		try {
			x.insertIntoTable(strTableName, htblColNameValue);
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	       		
	
	
	public static void  createTableParser(String[] tokens, DBApp x) throws DBAppException {
		Hashtable <String,String>  htblColNameType = new Hashtable <String,String> ( );
		Hashtable <String,String>  htblColNameMin = new Hashtable <String,String> ( );
		Hashtable <String,String>  htblColNameMax = new Hashtable <String,String> ( );
		
		String colName;
		String min = "";
		String max = "";
		String colType;
		String tableName = tokens[2];
		String clustering = "";
		for(int i = 3 ; i < tokens.length;i++) {
			if(tokens[i].equals("PRIMARY") && i+2 < tokens.length) {
				clustering = tokens[i+2];
				i+=3;
			}
			else {
				colName = tokens[i];
				colType = typeGetter(tokens[i+1]);
				System.out.println("COL NAME: " + colName + " COL Type: " + colType);
				htblColNameType.put(colName, colType);
				
				if(i+5 < tokens.length) {
				min = tokens[i+5];
				System.out.println(min + " min");
				}

				if(i+9 < tokens.length) {
				max = tokens[i+9];
				System.out.println(max + " max");
				}
				
				htblColNameMin.put(colName, min);
				htblColNameMax.put(colName, max);
				i = i+9;
			}
		}
		x.createTable(tableName, clustering , htblColNameType, htblColNameMin, htblColNameMax);
		
	}
	public static String typeGetter(String type) {
		//contains 3ashan lw fy bracket or comma or ay khara 
		if(type.contains("VARCHAR"))
			return "java.lang.String";
		if(type.contains("INT"))
			return "java.lang.Integer";
		if(type.contains("DECIMAL")) 
			return "java.lang.Double";	
		else
			return "java.lang.Date";	
	}
	
	public static void createIndexParser(String[] tokens, DBApp x) {
		String strTableName = tokens[4];
		String[] strarrColName;
		ArrayList<String> temp = new ArrayList<String>();
		for(int i = 5; i < tokens.length; i++) {
			temp.add(tokens[i]);
		}
		strarrColName = new String[temp.size()];
		for(int j = 0; j < temp.size(); j++) {
			strarrColName[j] = temp.get(j);
		}
		try {
			x.createIndex(strTableName, strarrColName);
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
//shahd was here ;p

	public static void deleteParser(String[] tokens, DBApp dbApp) {
		String strTableName = tokens[2];
		Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>();
		ArrayList<String> colNames = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		ArrayList<Object> valuesObj = new ArrayList<Object>();
		Hashtable<String,String> dataTypes = new Hashtable<String,String>();
		String val = "";
		try {
			dataTypes = StaticHelpers.getDataTypes(strTableName);
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int j = -1;
		for(int i = 4; i < tokens.length; i++) {
			j++;
			if(j == 0) {
				colNames.add(tokens[i]);
				//System.out.println("col names: " + tokens[i]);
			}
			else if(j == 2) {
				values.add(tokens[i]);
				//System.out.println("values " + tokens[i]);
			}
			else if(tokens[i].equals("AND")) {
				//System.out.println("test");
				j = -1;
			}
			}
		System.out.println(colNames.size());
		for(int k = 0; k < colNames.size(); k++) {
//			System.out.println(colNames.get(j) + "-----------");
			System.out.println(colNames.get(k) + "-----------------");
			val = dataTypes.get(colNames.get(k));
//			System.out.println(val);
			switch(val){
			case "java.lang.Integer": valuesObj.add(Integer.parseInt(values.get(k)));continue;
			case "java.lang.String": valuesObj.add(values.get(k));continue;	
			case "java.lang.Double": valuesObj.add(Float.parseFloat(values.get(k)));continue;
			case "java.lang.Date": SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			try {
				valuesObj.add(dateFormat.parse(values.get(k)));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}continue;
			}
		}
//		System.out.println(values.size());
		System.out.println(dataTypes.get("ID"));
		System.out.println(dataTypes.get("NAME"));
		System.out.println(colNames.size() + "  " + valuesObj.size());
		for(int k = 0; k < colNames.size(); k++) {
			htblColNameValue.put(colNames.get(k), valuesObj.get(k));
			System.out.println("col names: " + colNames.get(k) + " value: " + valuesObj.get(k));
		}
//		System.out.println(htblColNameValue.get("ID"));
//		System.out.println(htblColNameValue.get("DOB"));
//		System.out.println(htblColNameValue.get("NAME"));
		try {
			dbApp.deleteFromTable(strTableName, htblColNameValue);
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}			
	}

