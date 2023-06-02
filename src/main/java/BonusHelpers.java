package main.java;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class BonusHelpers {
	
	public static void populateValuesObj(ArrayList<String> colNames, ArrayList<String> values, ArrayList<Object> valuesObj, String tblName){
		Hashtable<String,String> dataTypes = new Hashtable<String,String>();
		try {
			dataTypes = StaticHelpers.getDataTypes(tblName);
		} catch (DBAppException e) {
			e.printStackTrace();
		}
		String val = "";
		
		System.out.println("cols: " + colNames.get(0));
//		System.out.println("cols: " + colNames.get(1));
		
		System.out.println(dataTypes.get("NAME"));
		
		for(int j = 0; j < colNames.size(); j++) {
			System.out.println(colNames.get(j) + "brrrrrrrrrrrrrrrrrrr");
			val = dataTypes.get(colNames.get(j));
			switch(val){
			case "java.lang.Integer": valuesObj.add(Integer.parseInt(values.get(j)));continue;
			case "java.lang.String": valuesObj.add(values.get(j));continue;	
			case "java.lang.Double": valuesObj.add(Float.parseFloat(values.get(j)));continue;
			case "java.lang.Date": SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			try {
				valuesObj.add(dateFormat.parse(values.get(j)));
			} catch (ParseException e) {
				e.printStackTrace();
			}continue;
			}
		}
	}
	
	public static Iterator selectParser(String[] tokens, DBApp dbApp) {
		
		String strTableName = "";

		String[] strarrOperators;
		strarrOperators = new String[tokens.length];
		SQLTerm[] SQLTerms;
		ArrayList<String> colNames = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		ArrayList<String> operators = new ArrayList<String>();
		ArrayList<String> arrOperators = new ArrayList<String>();
		ArrayList<Object> valuesObj = new ArrayList<Object>();
		int i = 1;
		int k = 0;
		for(i = 1; i< tokens.length; i++) {
			if(tokens[i].equalsIgnoreCase("From")) {
				strTableName = tokens[i+1];
			}
			if(tokens[i].equalsIgnoreCase("where")) {
				break;
			}	
		}
		System.out.println("table: "+ strTableName);
		for(int j = i + 1; j < tokens.length; j++) {
			colNames.add(tokens[j]);
			if(j + 2 < tokens.length) {
				operators.add(tokens[j + 1]);
				values.add(tokens[j + 2]);
			}
			if(j + 3 < tokens.length) {
				arrOperators.add(tokens[j + 3]);
				j += 3;
			}
			else {
				break;
			}
		}
		SQLTerms = new SQLTerm[operators.size()];
		strarrOperators = new String[arrOperators.size()];
		for(int h = 0; h < arrOperators.size(); h++) {
			strarrOperators[h] = arrOperators.get(h);
		}
		System.out.println(arrOperators.get(0));
		
		populateValuesObj(colNames,values,valuesObj,strTableName);
		
		for(int x = 0; x < SQLTerms.length; x++) {
			SQLTerms[x] = new SQLTerm();
			SQLTerms[x]._strTableName = strTableName;
			SQLTerms[x]._strOperator = operators.get(x);
			SQLTerms[x]._strColumnName = colNames.get(x);
			SQLTerms[x]._objValue = valuesObj.get(x);
			System.out.println("Table name: " + SQLTerms[x]._strTableName);
			System.out.println("Operator: " + SQLTerms[x]._strOperator);
			System.out.println("Column Name: " + SQLTerms[x]._strColumnName);
			System.out.println("object value: " + SQLTerms[x]._objValue);
		}
//		System.out.println("+++++++++++++++++++++"+strarrOperators[0]);
//		System.out.println("+++++++++++++++++++++"+strarrOperators[1]);
		try {
			dbApp.selectFromTable(SQLTerms, strarrOperators);
		} catch (DBAppException e) {

			e.printStackTrace();
		}
		Iterator resultSet = null;
		try {
			resultSet = dbApp.selectFromTable(SQLTerms , strarrOperators);
			System.out.println("Result:");
			while(resultSet.hasNext()) {
	          System.out.println(resultSet.next());
			}
			
		} catch (DBAppException e) {

			e.printStackTrace();
		}
		return resultSet;
					
	}

	public static void insertParser(String[] tokens, DBApp x) {
		String strTableName = tokens[2];
		ArrayList<String> colNames = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		ArrayList<Object> valuesObj = new ArrayList<Object>();
		Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>();
		boolean valuesCheck = false;
		
		for(int i = 3; i < tokens.length; i++) {
			if(tokens[i].equalsIgnoreCase("VALUES")) {
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
		
		populateValuesObj(colNames, values, valuesObj, strTableName);

			for(int k = 0; k < colNames.size(); k++) {
				
				htblColNameValue.put(colNames.get(k), valuesObj.get(k));
				System.out.println("colNames: "+ colNames.get(k) + " values:"+ valuesObj.get(k));
			}
		
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
			if(tokens[i].equalsIgnoreCase("PRIMARY") && i+2 < tokens.length) {
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
			
			e.printStackTrace();
		}
	}

	public static void deleteParser(String[] tokens, DBApp dbApp) {
		String strTableName = tokens[2];
		Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>();
		ArrayList<String> colNames = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		ArrayList<Object> valuesObj = new ArrayList<Object>();
		
		int j = -1;
		for(int i = 4; i < tokens.length; i++) {
			j++;
			if(j == 0) {
				colNames.add(tokens[i]);
			}
			else if(j == 2) {
				values.add(tokens[i]);
			}
			else if(tokens[i].equalsIgnoreCase("AND")) {
				j = -1;
			}
			}
		System.out.println(colNames.size());
		
		populateValuesObj(colNames, values, valuesObj, strTableName);
		
		System.out.println(colNames.size() + "  " + valuesObj.size());
		for(int k = 0; k < colNames.size(); k++) {
			htblColNameValue.put(colNames.get(k), valuesObj.get(k));
			System.out.println("col names: " + colNames.get(k) + " value: " + valuesObj.get(k));
		}
		try {
			dbApp.deleteFromTable(strTableName, htblColNameValue);
		} catch (DBAppException e) {
			
			e.printStackTrace();
		}
	}

	public static void updateParser(String[] tokens, DBApp dbApp) {
		String strTableName = tokens[1];
		Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>();
		ArrayList<String> colNames = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		ArrayList<Object> valuesObj = new ArrayList<Object>();
		String clustering = "";
		boolean whereCheck = false;
		int cols = 3;
		int vals = 5;
		boolean equalCheck = false;
		for(int i = 0; i <= tokens.length; i++) {
			if(tokens[i].equalsIgnoreCase("where")) {
				whereCheck = true;
				continue;
			}
	
			if(!whereCheck) {
				if(i == cols) {
					colNames.add(tokens[i]);
					cols += 3;
				}
				if(i == vals) {
					values.add(tokens[i]);
					vals += 3;
				}
			}
			else {
				
				if(tokens[i].equals("=")) {
					clustering = tokens[i+1];
					System.out.println("clustering: " + clustering);
					break;
				}
				
			}
		
		}
		System.out.println(colNames.size());
		
		populateValuesObj(colNames, values, valuesObj, strTableName);

		for(int k = 0; k < colNames.size(); k++) {
			
			htblColNameValue.put(colNames.get(k), valuesObj.get(k));
			System.out.println("colNames: "+ colNames.get(k) + " values:"+ valuesObj.get(k));
		}
		try {
			dbApp.updateTable(strTableName, clustering, htblColNameValue);
		} catch (DBAppException e) {
			e.printStackTrace();
		}
		
	}			
	}

