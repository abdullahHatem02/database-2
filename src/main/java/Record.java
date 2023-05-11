package main.java;
import java.io.Serializable;
import java.util.*;
public class Record implements Serializable {
	
	int page;
	int id;
	static int Gid;
	
	private static final long serialVersionUID = 1L;
	private Hashtable <String,Object> v;
	

	public Record(Hashtable <String,Object> v) {
		this.v = v;
		this.id = Gid++;
	}
	public Record(Hashtable <String,Object> v,int page) {
		this.v = v;
		this.id = Gid++;
		this.page =page;
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
	public int getPage() {
		return page;
	}
	public void setPage(int x) {
		page=x;
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
