package main.java;

import java.util.*;

public class Page {
	private static final long serialVersionUID = 1L;
	private Vector <Record> v;
	private int size;
	private String tableName;
	
	public Page(Vector <Record> v) {
		this.v = v;
		this.size = v.size();
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public Vector <Record> getV() {
		return v;
	}

	public void setV(Vector <Record> v) {
		this.v = v;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
