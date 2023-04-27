package main.java;
import java.io.Serializable;
import java.util.*;
public class Record implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Hashtable <String,Object> v;
	

	public Record(Hashtable <String,Object> v) {
		this.v = v;
		
	}
	public Hashtable <String,Object> getV() {
		return v;
	}
	public boolean compareRecords( Hashtable <String,Object> h) {
		Set <String> s = h.keySet();
		for(String s2 : s) {
			if(!h.get(s2).equals(v.get(s2)))
				return false;
		}
//		System.out.println("aaaa");
		return true;
	}
	
	public String toString() {
		return v.toString();
	}
	
//	public void setV(Vector <Object> v) {
//		this.v = v;
//	}
//	public String getTableName() {
//		return tableName;
//	}
//	public void setTableName(String tableName) {
//		this.tableName = tableName;
//	}
}