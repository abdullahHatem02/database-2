package main.java;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class Octree implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String name;
	Node root;
	String tableName;
	
	public Octree(String tableName,String name,Object [] boundsX, Object [] boundsY,Object [] boundsZ) {
		root = new Node(boundsX, boundsY,boundsZ,16,tableName,boundsX,boundsY,boundsZ);
//		System.out.println(boundsY[2] +"YYY");
		this.name = name;
		this.tableName = tableName;
		serialiazeOctree();
		
	}
	
	public void insert(Record r) throws DBAppException {
//		System.out.println(r + "hereoctet");
		
		root.insert(r);
		serialiazeOctree();
//		Node.deserialized = new Hashtable<String, Vector < Record>>();;
		System.gc();
	}
		
	
	public  void update(Object x, Object y, Object z, boolean x1, boolean y1, boolean z1, Hashtable <String, Object> hash) throws DBAppException {
		root.update(x, y, z, x1, y1, z1, hash);
//		root.print(0);
		serialiazeOctree();
		Node.deserialized = new Hashtable<String, Vector < Record>>();;
		System.gc();
	}
	
	public void delete(Object x, Object y, Object z, boolean x1, boolean y1, boolean z1, Hashtable <String, Object> hash) throws DBAppException {
		root.delete(x, y, z, x1, y1, z1,hash);
		serialiazeOctree();
//		Node.deserialized = new Hashtable<String, Vector < Record>>();;
		System.gc();
	}

	public List<Object>[] search(Object x, Object y, Object z, boolean x1, boolean y1, boolean z1) throws DBAppException {
		List<Object>[] res = root.search(x, y, z, x1, y1, z1);
		serialiazeOctree();
//		Node.deserialized = new Hashtable<String, Vector < Record>>();;
		System.gc();
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
	public static void deleteIndex(String name) {
		File file = new File("src/main/resources/data/" + name  +".ser");
		System.out.println(file.delete());
	}
}

//select
//SQL TERM
//CREATE INDEX
//MOSHKELA NESAMA3 FI INSERT/DELETE
//ADD PAGE NUMBER TO RECORDS
//BONUS
