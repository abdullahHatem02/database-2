package main.java;

import java.io.Serializable;

public class Null implements Serializable, Comparable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "null";
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return -1;
	}
}
