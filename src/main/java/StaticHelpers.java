package main.java;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

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
	    public static Hashtable <String,String> checkIndex(String tableName) throws IOException {
	        File inputFile = new File("src/main/resources/metadata.csv");

	        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
	        Hashtable <String,String> res = new Hashtable <String,String>();
	        String line;
	        while ((line = reader.readLine()) != null) {
	            String[] columns = line.split(",");
//	            System.out.println(columns);
	            if( columns[0].equals(tableName) && columns[5].equals("Octree")) {
	            	res.put(columns[1], columns[4]);
	            }
	        }
	        reader.close();
	        return res;
	        
	    }
	    public static Vector<Record> binarySearchPK(Object fromTarget,Object tillTarget, boolean exact ,ArrayList <String> pkInfo, String tableName) throws DBAppException {
			Vector<Record> res = new Vector<Record>();
			boolean flag = false;
			try {
			ObjectInputStream in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + tableName + ".ser"));
			Table table = (Table) in.readObject();
			in.close();
			if(table.getPages().size()==0) 
				return res;
			int low = 0;
	        int i = 0;
	        int mid = 0;
	        for(i =0; i<table.getPages().size();i++) {
	             ObjectInputStream pageIn = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + tableName + table.getPages().get(i) + ".ser"));
	             Vector <Record> vector = (Vector <Record>) pageIn.readObject();
	             low = 0;
	             mid = 0;
		        int high = vector.size() - 1; //kam rec fl page
		        while (low <= high) {
		            mid = (low + high) / 2;
		           
		            Object current = vector.get(mid).getV().get(pkInfo.get(1));
//		            System.out.println(current);
		            if  (((Comparable<Object>)current).compareTo(fromTarget) == 0) {
		            	res.add((Record)vector.get(mid));
//		            	 System.out.println(res +"yy");
		            	if(exact) 
		            		return res;
		            	Object curr = vector.get(++mid).getV().get(pkInfo.get(1));
		            	while(((Comparable<Object>)curr).compareTo(tillTarget)!=0) {
		            		res.add((Record)vector.get(mid));
		            		mid++;
		            	}
		            	in.close();
		            	pageIn.close();
		                return res;
		            }else if (((Comparable<Object>)current).compareTo(fromTarget) < 0) {
		                low = mid + 1;
		            } 
		            else {
		                high = mid - 1;
		            	}
		        	}
		        pageIn.close();
		        if((((Comparable<Object>)vector.lastElement().getV().get(pkInfo.get(1))).compareTo(fromTarget) > 0)) {
		        	flag = true;
		        	break;
		        }
		        pageIn = null;
		        System.gc();
		        }
	       
				return res;
				
	        }
			catch(Exception e) {
							e.printStackTrace();
			 throw new DBAppException("Binary search btala3 records");
			}
		}
	//minn w maxx mn arrayList pkInfo (2)(3)
	public static ArrayList <Object> decipher (SQLTerm term, String minn, String maxx,String type){
		//3ayzeen el hash bta3 el min w el max 
		String colName= term._strColumnName;
		String operation= term._strOperator; 
		Object from = null;
		Object to = null;
		Object min = null;
		Object max = null;
		try {
			System.out.println(minn);
			min = DBApp.parsePrimaryKey(type, minn);
			max = DBApp.parsePrimaryKey(type, maxx);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		boolean exact=false;
		boolean needBS=true;
		//needBS, exact, from, to
		ArrayList <Object> res = new ArrayList<Object>();
		switch(operation) {
		case"=": exact=true;from =term._objValue; break;
		case">": from = incValue(term._objValue); to=max; break;
		case">=": from = term._objValue; to=max; break;
		case"<": from= min; to= decValue(term._objValue); break;
		case"<=": from= min; to= term._objValue;break;
		case"!=": needBS=false;
		}
		res.add(needBS);
		res.add(exact);
		res.add(from);
		res.add(to);
		return res;
	}
	
	public static Object incValue(Object o) {
		if(o instanceof Double)
			o = (Double)o+Double.MIN_VALUE;
		if(o instanceof Date) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); // create SimpleDateFormat object
	        try {
	            Calendar cal = Calendar.getInstance(); // create Calendar object
	            cal.setTime((Date)o); // set Calendar object's time to the input date
	            cal.add(Calendar.DAY_OF_MONTH, 1); // increment date by 1 day
	            Date nextDay = cal.getTime(); // get the incremented date
	            String nextDayStr = sdf.format(nextDay); // format incremented date to string
	            o = sdf.parse(nextDayStr);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		}
		if(o instanceof Integer)
			o = (Integer)o+1;
		if(o instanceof String) {
			char[] chars = ((String)o).toCharArray();
	        int lastIndex = chars.length - 1; // get the index of the last character
	        if (chars[lastIndex] == 'z') { // handle special case of 'z' by replacing with 'a'
	            chars[lastIndex] = 'a'; //"dz" ,, "ez" handle this case
	        } else { // increment last character by 1
	            chars[lastIndex]++;
	        }
	        o =  new String(chars);
		}
		return o;
	}
	
	public static Object decValue(Object o) {
		if(o instanceof Double)
			o = (Double)o-Double.MIN_VALUE;
		if(o instanceof Date) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); // create SimpleDateFormat object
	        try {
	            Calendar cal = Calendar.getInstance(); // create Calendar object
	            cal.setTime((Date)o); // set Calendar object's time to the input date
	            cal.add(Calendar.DAY_OF_MONTH, -1); // decrement date by 1 day
	            Date nextDay = cal.getTime(); // get the decremented date
	            String nextDayStr = sdf.format(nextDay); // format decremented date to string
	            o = sdf.parse(nextDayStr);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		}
		if(o instanceof Integer)
			o = (Integer)o-1;
		if(o instanceof String) {
			char[] chars = ((String)o).toCharArray();
	        int lastIndex = chars.length - 1; // get the index of the last character
	        if (chars[lastIndex] == 'a') { // handle special case of 'a' by replacing with 'z'
	            chars[lastIndex] = 'z';
	        } else { // decrement last character by 1
	            chars[lastIndex]--;
	        }
	        o =  new String(chars);
		}
		return o;
	}
	
	public static ArrayList <String> getMaxMinVals(String strTableName,String colName) throws DBAppException {
		
		try {
			FileReader fr = new FileReader("src/main/resources/metadata.csv");
			BufferedReader br = new BufferedReader(fr);
			ArrayList <String> res = new ArrayList<String>(); 
			String s = br.readLine();
			String max="";
			String min="";
			while(s != null) {
				if(s.split(",")[0].equals(strTableName) && s.split(",")[1].equals(colName)) {
					min=s.split(",")[6];
					max=s.split(",")[7];
					break;
				}
				s = br.readLine();	
			}
			br.close();
			res.add(min);
			res.add(max);
			return res;	
		} catch (Exception e) {
			throw new DBAppException(e.getMessage());
		}
	}
	
	public static Vector <Record> linearSelect(Vector <Record> v, SQLTerm [] arrSQLTerms,String [] strarrOperators  ) throws DBAppException {
		Vector <Record> result = new Vector <Record>();
		 for (int i = 0; i < v.size(); i++) {
		        Record record = v.elementAt(i);
		        boolean satisfiesTerms = strarrOperators!= null && strarrOperators.length >0? false:true;
		for (int j = 0; j < arrSQLTerms.length; j++) {
	            SQLTerm term = arrSQLTerms[j];
            Object value = record.getV().get(term._strColumnName);
//            System.out.println(value);
            switch (term._strOperator) {
            //if str[0] === AND satsis = sats &&,||,^  ((Comparable) value).compareTo(term._objValue) > 0;
                case ">":
                    satisfiesTerms = j==0? ((Comparable) value).compareTo(term._objValue) > 0 : 
                    	DBApp.performOperation(strarrOperators[j-1], satisfiesTerms,((Comparable) value).compareTo(term._objValue) > 0 );
                    break;
                case ">=":
                	satisfiesTerms = j==0? ((Comparable) value).compareTo(term._objValue) >= 0 : 
                		DBApp.performOperation(strarrOperators[j-1], satisfiesTerms,((Comparable) value).compareTo(term._objValue) >= 0 );
                    break;
                case "<":
                	satisfiesTerms = j==0? ((Comparable) value).compareTo(term._objValue) < 0 : 
                		DBApp.performOperation(strarrOperators[j-1], satisfiesTerms,((Comparable) value).compareTo(term._objValue) < 0 );
                    break;
                case "<=":
                	satisfiesTerms = j==0? ((Comparable) value).compareTo(term._objValue) <= 0 : 
                		DBApp.performOperation(strarrOperators[j-1], satisfiesTerms,((Comparable) value).compareTo(term._objValue) <= 0 );
                    break;
                case "!=":
                	satisfiesTerms = j==0? ((Comparable) value).compareTo(term._objValue) != 0 : 
                		DBApp.performOperation(strarrOperators[j-1], satisfiesTerms,((Comparable) value).compareTo(term._objValue) != 0 );
                    break;
                case "=":
                	satisfiesTerms = j==0? ((Comparable) value).compareTo(term._objValue) == 0 : 
                		DBApp.performOperation(strarrOperators[j-1], satisfiesTerms,((Comparable) value).compareTo(term._objValue) == 0 );
                    break;
            }
//            System.out.println(satisfiesTerms);
        }
        if (satisfiesTerms) {
//        	System.out.println(record.getV());
            result.add(record);
        }
        
    }
		 return result;
		}

}
