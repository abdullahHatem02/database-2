package main.java;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.List;

public class Octree implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String name;
	Node root;
	
	public Octree(String name,Object [] boundsX, Object [] boundsY,Object [] boundsZ) {
		root = new Node(boundsX, boundsY,boundsZ,4);
		this.name = name;
	}
	public void insert(Record r) {
		root.insert(r);
		serialiazeOctree();
	}
		
	
	public  void update(Object x, Object y, Object z, boolean x1, boolean y1, boolean z1, Hashtable <String, Object> hash) {
		root.update(x, y, z, x1, y1, z1, hash);
		serialiazeOctree();
	}
	public void delete(Object x, Object y, Object z, boolean x1, boolean y1, boolean z1, Hashtable <String, Object> hash) {
		root.delete(x, y, z, x1, y1, z1,hash);
		serialiazeOctree();
	}
	public List<Object>[] search(Object x, Object y, Object z, boolean x1, boolean y1, boolean z1) {
		List<Object>[] res = root.search(x, y, z, x1, y1, z1);
		serialiazeOctree();
		return res;
	}
	public void serialiazeOctree() {
		 
		 try {
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + this.name +".ser"));
			out.writeObject(this);
			 out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static Octree deserialiazeOctree(String name) { //use in DBAPP class
		Octree r = null;
		 try {
			ObjectInputStream in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + name +".ser"));
			 r = (Octree )in.readObject();
			in.close();
			return r;
			
		} catch (IOException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return r;
		
	}
}

//select
//SQL TERM
//BONUS
//CREATE INDEX
//MOSHKELA NESAMA3 FI INSERT/DELETE
//INSERT PK OR INDEX??
//ADD PAGE NUMBER TO RECORDS
