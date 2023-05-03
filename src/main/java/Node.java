package main.java;
import java.util.*;

import main.java.Record;

public class Node {
//    private final int depth;
    private Object[] boundsX;
    private Object[] boundsY;
    private Object[] boundsZ;
    private Node[] children;
    private List <List <Record>> entries; // to handle duplicates
    private final int maxEntries;
    
    public Node(Object[] boundsX, Object[] boundsY, Object[] boundsZ, int maxEntries) {
		super();
		this.boundsX = boundsX;
		this.boundsY = boundsY;
		this.boundsZ = boundsZ;
		this.maxEntries = maxEntries;
		 entries = new ArrayList <List <Record>>();
		this.children = null;
	}
    
    public void insert(Record r) {
    	if(this.children== null) {
    	
    	if(maxEntries == entries.size()) { //ehna la2ena el 3ayel bas hoa malyan :3 
    		Object midX = getMidValue(boundsX[1], boundsX[2]);
    		Object midY = getMidValue(boundsY[1], boundsY[2]);
    		Object midZ = getMidValue(boundsZ[1], boundsZ[2]);
    		this.children = new Node [8];
    		
    		Object [] boundsXLower = {boundsX[0], boundsX[1],midX}; //a
    		Object [] boundsXUpper = {boundsX[0],midX,boundsX[2]}; //b
    		
    		Object [] boundsYLower = {boundsY[0], boundsY[1],midY}; //c
    		Object [] boundsYUpper = {boundsY[0],midY,boundsY[2]};  //d
    		
    		Object [] boundsZLower = {boundsZ[0], boundsZ[1],midZ}; //e
    		Object [] boundsZUpper = {boundsZ[0],midZ,boundsZ[2]}; //f
    		
    		children [0] = new Node(boundsXLower,boundsYLower,boundsZLower,16);
    		children [1] = new Node(boundsXLower,boundsYLower,boundsZUpper,16);
    		children [2] = new Node(boundsXLower,boundsYUpper,boundsZLower,16);
    		children [3] = new Node(boundsXLower,boundsYUpper,boundsZUpper,16);
    		children [4] = new Node(boundsXUpper,boundsYLower,boundsZLower,16);
    		children [5] = new Node(boundsXUpper,boundsYLower,boundsZUpper,16);
    		children [6] = new Node(boundsXUpper,boundsYUpper,boundsZLower,16);
    		children [7] = new Node(boundsXUpper,boundsYUpper,boundsZUpper,16);
    		
    		for(int i = 0; i<entries.size();i++) {
    			Object x = entries.get(i).get(0).getV().get(boundsX[0]);
    			Object y = entries.get(i).get(0).getV().get(boundsY[0]);
    			Object z = entries.get(i).get(0).getV().get(boundsZ[0]);
    			for(int j = 0; j<children.length; j++) {
    				if(compareObjects(x, children[j].boundsX[1]) >= 0  && compareObjects(x, children[j].boundsX[2]) < 0 &&
    						compareObjects(y, children[j].boundsY[1]) >= 0 && compareObjects(y, children[j].boundsY[2]) < 0 &&
    						compareObjects(z, children[j].boundsZ[1]) >= 0 && compareObjects(z, children[j].boundsZ[2]) < 0) {
    					children[j].entries.add(entries.get(i));
    				}
    			}
    			
    		}
    		Object x = r.getV().get(boundsX[0]);
			Object y = r.getV().get(boundsY[0]);
			Object z = r.getV().get(boundsZ[0]);
			for(int j = 0; j<children.length; j++) {
				if(compareObjects(x, children[j].boundsX[1]) >= 0  && compareObjects(x, children[j].boundsX[2]) < 0 &&
						compareObjects(y, children[j].boundsY[1]) >= 0 && compareObjects(y, children[j].boundsY[2]) < 0 &&
						compareObjects(z, children[j].boundsZ[1]) >= 0 && compareObjects(z, children[j].boundsZ[2]) < 0) {
					List <Record> n = new ArrayList<>();
	    			n.add(r);
					children[j].entries.add(n);
				}
			}
    		this.entries = null;
    	}
    	else {
    		Hashtable <String,Object> x = r.getV();
    		boolean flag = false;
    		for(int i = 0;i<entries.size();i++) {
    			if(entries.get(i) != null)
	    			if(x.get(boundsX[0]).equals(entries.get(i).get(0).getV().get(boundsX[0])) && x.get(boundsY[0]).equals(entries.get(i).get(0).getV().get(boundsY[0])) && x.get(boundsZ[0]).equals(entries.get(i).get(0).getV().get(boundsZ[2]))) {
	    				entries.get(i).add(r);
	    				flag = true;
	    				break;
	    			}	
    		
    		}
    		if(!flag) {
    			List <Record> n = new ArrayList<>();
    			n.add(r);
    			entries.add(n);
    			}
    	}
    	}
    	else {
    		Object x = r.getV().get(boundsX[0]);
			Object y = r.getV().get(boundsY[0]);
			Object z = r.getV().get(boundsZ[0]);
			for(int j = 0; j<children.length; j++) {
				if(compareObjects(x, children[j].boundsX[1]) >= 0  && compareObjects(x, children[j].boundsX[2]) < 0 &&
						compareObjects(y, children[j].boundsY[1]) >= 0 && compareObjects(y, children[j].boundsY[2]) < 0 &&
						compareObjects(z, children[j].boundsZ[1]) >= 0 && compareObjects(z, children[j].boundsZ[2]) < 0) {
					List <Record> n = new ArrayList<>();
	    			n.add(r);
					children[j].entries.add(n);
				}
			}
			
    	}
    }
    
    public static <T extends Comparable<T>> T getMidValue(Object bounds2, Object bounds3) {
      
        
        if (bounds2 instanceof Date) {
            long midTime = ((Date) bounds2).getTime() + (((Date) bounds3).getTime() - ((Date) bounds2).getTime()) / 2;
            return (T) new Date(midTime);
        } else if (bounds2 instanceof Integer) {
            int mid = (int) Math.round((double) ((Integer) bounds2 + (Integer) bounds3) / 2);
            return (T) Integer.valueOf(mid);
        } else if (bounds2 instanceof Double) {
            double mid = ((Double) bounds2 + (Double) bounds3) / 2;
            return (T) Double.valueOf(mid);
        } else  {
        	int N = ((String) (bounds2)).length();
        	String S = ((String) (bounds2));
        	String T = ((String) (bounds3));
        	int[] a1 = new int[N + 1];
        	 
            for (int i = 0; i < N; i++) {
                a1[i + 1] = (int)S.charAt(i) - 97
                            + (int)T.charAt(i) - 97;
            }
            for (int i = N; i >= 1; i--) {
                a1[i - 1] += (int)a1[i] / 26;
                a1[i] %= 26;
            }
            for (int i = 0; i <= N; i++) {
                if ((a1[i] & 1) != 0) {
     
                    if (i + 1 <= N) {
                        a1[i + 1] += 26;
                    }
                }
     
                a1[i] = (int)a1[i] / 2;
            }
            String s = "";
            for (int i = 1; i <= N; i++) {
                s += (char)(a1[i] + 97);
            }
            return (T) s;
        }
     
        }
    public int compareObjects(Object obj1, Object obj2) {
        if (obj1 instanceof Double && obj2 instanceof Double) {
            return Double.compare((Double) obj1, (Double) obj2);
        } else if (obj1 instanceof String && obj2 instanceof String) {
            return ((String) obj1).compareTo((String) obj2);
        } else if (obj1 instanceof Integer && obj2 instanceof Integer) {
            return Integer.compare((Integer) obj1, (Integer) obj2);
        } else  {
            return ((Date) obj1).compareTo((Date) obj2);
        } 
    }

    
    public List<Object>[] search(Object x, Object y, Object z, boolean x1, boolean y1, boolean z1) {
//    	Object x = r.getV().get(boundsX[0]);
//		Object y = r.getV().get(boundsY[0]);
//		Object z = r.getV().get(boundsZ[0]);
    	 List<Object>[] m = new List[2];
    	List<List<Record>> res = new ArrayList<>();
    	List<Node> res2 = new ArrayList<>();
		for(int j = 0; j<children.length; j++) {
			if(compareObjects(x, children[j].boundsX[1]) >= 0  && compareObjects(x, children[j].boundsX[2]) < 0 &&
					compareObjects(y, children[j].boundsY[1]) >= 0 && compareObjects(y, children[j].boundsY[2]) < 0 &&
					compareObjects(z, children[j].boundsZ[1]) >= 0 && compareObjects(z, children[j].boundsZ[2]) < 0) {
				for(int i=0; i<children[j].entries.size(); i++) {
					if( (compareObjects(x, children[j].entries.get(i).get(0).getV().get(boundsX[0])) == 0 || !x1) && 
							(compareObjects(y, children[j].entries.get(i).get(0).getV().get(boundsY[0])) == 0 || !y1) && 
							(compareObjects(z, children[j].entries.get(i).get(0).getV().get(boundsZ[0])) == 0 || !z1) ) {
						res.add(children[j].entries.get(i));
						res2.add(children[j]);
					}
				}
			}
		}
		
		 m[0] = (List<Object>) (List<?>) res;
		 m[1] = (List<Object>) (List<?>) res2;
	
		
		return m;
    }
    
    public void update(Object x, Object y, Object z, boolean x1, boolean y1, boolean z1, Hashtable <String, Object> hash) {
    	List<Object> [] result = search(x,y,z,x1,y1,z1);
    	List<List<Record>> recs = (List<List<Record>>) (List<?>) result[0];
    	List<Node> node = (List<Node>) (List<?>) result[1];
    	
    	for(int i = 0; i<recs.size(); i++) {
    		for(int j=0; j<recs.get(i).size(); i++){
    			Record r = recs.get(i).get(j);
    			Set<String> set = hash.keySet();
    			for(String s:set) {
    				r.getV().put(s, hash.get(s));
    			}
    			for(int k=0; k<node.size(); k++) {
    				for(int n=0; n<node.get(k).entries.size(); n++) {
    					if(node.get(k).entries.get(n) == r) {
    						node.get(k).entries.remove(n);
    						insert(r);
    					}
    				}
    			}
    		}
    	}
    }
    public List<List<Record>> delete(Object x, Object y, Object z, boolean x1, boolean y1, boolean z1, Hashtable <String, Object> hash) {
    	//Hahstable is missing
    	List<Object> [] result = search(x,y,z,x1,y1,z1);
    	List<List<Record>> recs = (List<List<Record>>) (List<?>) result[0];
    	List<Node> node = (List<Node>) (List<?>) result[1];
    	
    	for(int i = 0; i<recs.size(); i++) {
    		for(int j=0; j<recs.get(i).size(); i++){
    			Record r = recs.get(i).get(j);
    			Set<String> set = hash.keySet();
    			//boolean remove = false; //remove mn el LIST el han-remove mnhaa (f e3kesy e l logic) ~Youstina
    			for(String s:set) {
    				if (compareObjects(r.getV().get(s), hash.get(s)) != 0){
    						recs.get(i).remove(j);
    						 break;
    				}
    			}
    			for(int k=0; k<node.size(); k++) {
    				for(int n=0; n<node.get(k).entries.size(); n++) {
    					if(node.get(k).entries.get(n) == r) {
    						node.get(k).entries.remove(n);
    					}
    				}
    			}
    		}
    	}
    	return recs; //3shan ne3mlhom remove ml table b2aa
    }
}
