package main.java;

import java.util.Vector;

public class Table implements java.io.Serializable {
	
	private static final long serialVersionUID = 1L;
	private int rows;
	private Vector <Integer> pages;
	private String tableName;
	private int columns;
	
	public Table(int rows,String tableName) {
		this.rows = rows;
		this.pages = new Vector <Integer>() ;
		this.tableName = tableName;
	}

	public int getRows() {
		return rows;
	}

	public void setRows(int rows) {
		this.rows = rows;
	}

	public Vector <Integer> getPages() {
//		System.out.println(pages);
		return pages;
	}

	public void setPages(Vector <Integer> pages) {
		this.pages = pages;
	}

	public String getTableName() {
		return tableName;
	}

	public int getColumns() {
		return columns;
	}

	public void setColumns(int columns) {
		this.columns = columns;
	}

}
