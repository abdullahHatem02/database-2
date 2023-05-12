package main.java;
import java.awt.Point;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

import main.java.Record;

public class Node implements Serializable {
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
//    private final int depth;
    Object[] boundsX;
    Object[] boundsY;
    Object[] boundsZ;
    private String tableName;
    private Node[] children;
    private transient Vector <Vector <Record>> entries; // to handle duplicates
    private Vector <Vector <Point>> entriesIdPage;
    
    private final int maxEntries;
    
    public Node(Object[] boundsX, Object[] boundsY, Object[] boundsZ, int maxEntries,String tableName) {
		super();
		this.boundsX = boundsX;
		this.boundsY = boundsY;
		this.boundsZ = boundsZ;
		this.maxEntries = maxEntries;
		entries = new Vector <Vector <Record>>();
		entriesIdPage = new Vector <Vector <Point>>();
		this.children = null;
		this.tableName = tableName;
	}

    public void insert(Record r) throws DBAppException {
    	if(this.children== null) {
    		this.entries = new Vector<Vector<Record>>();
    		this.deserializeNodeEntries();
    		Hashtable <String,Object> x = r.getV();
    		boolean flagDuplicate = false;
    		for(int i = 0;i<entries.size();i++) {
    			if(entries.get(i) != null)
	    			if(x.get(boundsX[0]).equals(entries.get(i).get(0).getV().get(boundsX[0]))
	    					&& x.get(boundsY[0]).equals(entries.get(i).get(0).getV().get(boundsY[0])) 
	    					&& x.get(boundsZ[0]).equals(entries.get(i).get(0).getV().get(boundsZ[0]))) {
	    				entries.get(i).add(r);
	    				entriesIdPage.get(i).add(new Point(r.id,r.page));
	    				flagDuplicate = true;
	    				break;
	    			}	
    		}
    		if(!flagDuplicate) {
    			Vector <Record> n = new Vector<Record>();
    			n.add(r);
    			entries.add(n);
    			Vector <Point> n2 = new Vector<Point>();
    			n2.add(new Point(r.id,r.page));
    			entriesIdPage.add(n2);
//    			serializePage();
    			}
    	if(maxEntries < entries.size()) { //ehna la2ena el 3ayel bas hoa malyan :3 
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
    		
    		children [0] = new Node(boundsXLower,boundsYLower,boundsZLower,16,tableName);
    		children [1] = new Node(boundsXLower,boundsYLower,boundsZUpper,16,tableName);
    		children [2] = new Node(boundsXLower,boundsYUpper,boundsZLower,16,tableName);
    		children [3] = new Node(boundsXLower,boundsYUpper,boundsZUpper,16,tableName);
    		children [4] = new Node(boundsXUpper,boundsYLower,boundsZLower,16,tableName);
    		children [5] = new Node(boundsXUpper,boundsYLower,boundsZUpper,16,tableName);
    		children [6] = new Node(boundsXUpper,boundsYUpper,boundsZLower,16,tableName);
    		children [7] = new Node(boundsXUpper,boundsYUpper,boundsZUpper,16,tableName);
    		
    		for(int i = 0; i<entries.size();i++) {
    			Object x1 = entries.get(i).get(0).getV().get(boundsX[0]);
    			Object y = entries.get(i).get(0).getV().get(boundsY[0]);
    			Object z = entries.get(i).get(0).getV().get(boundsZ[0]);
    			for(int j = 0; j<children.length; j++) {
    				if(compareObjects(x1, children[j].boundsX[1]) >= 0  && compareObjects(x1, children[j].boundsX[2]) < 0 &&
    						compareObjects(y, children[j].boundsY[1]) >= 0 && compareObjects(y, children[j].boundsY[2]) < 0 &&
    						compareObjects(z, children[j].boundsZ[1]) >= 0 && compareObjects(z, children[j].boundsZ[2]) < 0) {
    					children[j].entries.add(entries.get(i));
    					children[j].entriesIdPage.add(entriesIdPage.get(i));
    				}
    			}
    		}
    		this.entries = null;
    	}
    	else {
    		
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
					children[j].insert(r);
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
        	String S = ((String) (bounds2)).toLowerCase();
        	String T = ((String) (bounds3)).toLowerCase();
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
        	
            return (((String) obj1).toLowerCase()).compareTo(((String) obj2).toLowerCase());
        } else if (obj1 instanceof Integer && obj2 instanceof Integer) {
        	
            return Integer.compare((Integer) obj1, (Integer) obj2);
        } else  {
            return ((Date) obj1).compareTo((Date) obj2);
        } 
    }

    
    public List<Object>[] search(Object x, Object y, Object z, boolean x1, boolean y1, boolean z1) throws DBAppException {
    	 List<Object>[] m = new List[2];
    	List<List<Record>> res = new ArrayList<>();
    	List<Node> res2 = new ArrayList<>();
		for(int j = 0; j<children.length; j++) {
			if( (  !x1 || compareObjects(x, children[j].boundsX[1]) >= 0)  && ( !x1 || compareObjects(x, children[j].boundsX[2]) < 0) &&
					(!y1 || compareObjects(y, children[j].boundsY[1]) >= 0) && (!y1 || compareObjects(y, children[j].boundsY[2]) < 0) &&
					(!z1 || compareObjects(z, children[j].boundsZ[1]) >= 0) && (!z1 || compareObjects(z, children[j].boundsZ[2]) < 0)) {
				if(children[j].children != null) {
					res.addAll( (ArrayList)  children[j].search(x, y, z, x1, y1, z1)[0]);
					res2.addAll( (ArrayList)  children[j].search(x, y, z, x1, y1, z1)[1]);
				}
				else {
					if(children[j].entriesIdPage != null && children[j].entries == null) {
						children[j].entries = new Vector<Vector<Record>>();
						this.children[j].deserializeNodeEntries();}
				for(int i=0; i<children[j].entries.size(); i++) {
					if(children[j].entries.get(i).size()>0)
						
					if( ( !x1 || compareObjects(x, children[j].entries.get(i).get(0).getV().get(boundsX[0])) == 0 || !x1) && 
							( !y1 || compareObjects(y, children[j].entries.get(i).get(0).getV().get(boundsY[0])) == 0 || !y1) && 
							(!z1 || compareObjects(z, children[j].entries.get(i).get(0).getV().get(boundsZ[0])) == 0 || !z1) ) {
						res.add(children[j].entries.get(i));
						if(!res2.contains(children[j]))
							res2.add(children[j]);
				}}
			}
		}}
		 m[0] = (List<Object>) (List<?>) res;
		 m[1] = (List<Object>) (List<?>) res2;
		 
		
		return m;
    }
      
    public void update(Object x, Object y, Object z, boolean x1, boolean y1, boolean z1, Hashtable <String, Object> hash) throws DBAppException {
    	List<Object> [] result = search(x,y,z,x1,y1,z1);
    	List<List<Record>> recs = (List<List<Record>>) (List<?>) result[0];
    	List<Node> node = (List<Node>) (List<?>) result[1];
    	
    	for(int i = 0; i<recs.size(); i++) {
    		for(int j=0; j<recs.get(i).size(); j++){
    			Record r = recs.get(i).get(j);
    			Set<String> set = hash.keySet();
    			for(String s:set) {
    				r.getV().put(s, hash.get(s));
    			}
    			for(int k=0; k<node.size(); k++) {
    				if(node.get(k).entries == null) {
    					node.get(k).entries = new Vector<Vector<Record>>();
    					node.get(k).deserializeNodeEntries();
    				}
    				for(int n=0; n<node.get(k).entries.size(); n++) {
    					node.get(k);
    					node.get(k).entries.get(n);
    					boolean flagDeleted = false;
    					for(int l = 0;l<node.get(k).entries.get(n).size();l++) {
	    					if(node.get(k).entries.get(n).get(l) == r) {
	    						node.get(k).entries.get(n).remove(l);
	    						node.get(k).entriesIdPage.get(n).remove(l);
	    						if(node.get(k).entries.get(n).size() ==0) {
	            					node.get(i).entries.remove(n);
	            					node.get(i).entriesIdPage.remove(n);
	            					
	            					flagDeleted = true;
	            				}
	    						serializePage();
	    						insert(r);
	    						
	    						
	    						break;
	    					}
    					}
    					if(flagDeleted == true) {n--;break;}
    				}
    			}
    		}
    	}
    }
	
    private void serializePage() throws DBAppException {
		// TODO Auto-generated method stub
		if(deserialized == null)
			return;
		try {
			Set <String> set =  deserialized.keySet();
		for(String s: set) {
			ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("src/main/resources/data/" + s +  ".ser"));
			outputStream.writeObject(deserialized.get(s));
			outputStream.close();
			}
		deserialized = new Hashtable<String, Vector<Record>>();
		}
		catch(Exception e) {
			throw new DBAppException();
		}	
	}
	
	public List<List<Record>> delete(Object x, Object y, Object z, boolean x1, boolean y1, boolean z1, Hashtable <String, Object> hash) throws DBAppException {
    	//Hahstable is missing
    	List<Object> [] result = search(x,y,z,x1,y1,z1);
    	List<List<Record>> recs = (List<List<Record>>) (List<?>) result[0];
    	List<Node> node = (List<Node>) (List<?>) result[1];
    	
    	for(int i = 0; i<recs.size(); i++) {
    		
    		for(int j=0; j<recs.get(i).size(); j++){
    			
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
    					node.get(k);
    					node.get(k).entries.get(n);
    					for(int l = 0;l<node.get(k).entries.get(n).size();l++) {
	    					if(node.get(k).entries.get(n).get(l) == r) {
	    						
	    			    		deserialized.get(tableName+r.page+"").remove(r);
	    						node.get(k).entries.get(n).remove(l);
	    						node.get(k).entriesIdPage.get(n).remove(l);
	    						System.out.println(recs);
	    						l--;
	    						
	    					}
    					}
    					if(node.get(k).entries.get(n).size() ==0) {
    						System.out.println(recs);
        					node.get(i).entries.remove(n);
        					node.get(i).entriesIdPage.remove(n);
        					n--;
        					System.out.println(recs);
        				}
    				}	
    			}
    			
    		}
    	}
    	serializePage();	
    	return recs; //3shan ne3mlhom remove ml table b2aa
    }
    
	public void print(int level) throws DBAppException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < level; i++) {
            sb.append("\t");
        }
        String padding = sb.toString();

        System.out.println(padding + "BoundsX: " + Arrays.toString(boundsX));
        System.out.println(padding + "BoundsY: " + Arrays.toString(boundsY));
        System.out.println(padding + "BoundsZ: " + Arrays.toString(boundsZ));
        System.out.println(padding + "Max Entries: " + maxEntries);
        if(children == null && entriesIdPage != null) {
        	entries = new Vector<Vector<Record>>();
        	this.deserializeNodeEntries();}
        System.out.println(padding + "Entries: " + entries);

        if (children != null) {
            for (int i = 0; i < 8; i++) {
                System.out.println(padding + "- Level " + (level + 1) + ":");
                children[i].print(level + 1);
            }
        }
    }
   
    static Hashtable <String,Vector <Record>> deserialized = new Hashtable<String, Vector < Record>>();//Make sure that this is allowed
    
    public void deserializeNodeEntries() throws DBAppException {
    	if(entries!=null && this.entries.size() == this.entriesIdPage.size()) {
    		return;
    	}
    	HashSet <Integer> pages = new HashSet <Integer>();
    	for(int i = 0;i<entriesIdPage.size();i++) {
    		for(int j = 0;j<entriesIdPage.get(i).size();j++) {
    			pages.add(entriesIdPage.get(i).get(j).y);
    		}
    	}
    	try {
    		
    	for(int i: pages) {
    		Vector <Record> page;
    		if(deserialized.get(tableName + i) == null) {
    		ObjectInputStream in = new ObjectInputStream(new FileInputStream("src/main/resources/data/" + tableName + ".ser"));
			Table table = (Table) in.readObject();
			page = DBApp.deserializePage(table,i);
//			toSerialize = page;
			deserialized.put(tableName +i, page);}
    		else {
    			page = deserialized.get(tableName + i);
    		}
			
			for(Record r : page) {
				for(int j = 0;j<entriesIdPage.size();j++) {
					if(j>=entries.size())
						entries.add(j,new Vector <Record>());
		    		for(int k = 0;k<entriesIdPage.get(j).size();k++) {
		    			
		    			if(r.id == entriesIdPage.get(j).get(k).x) {
		    				entries.get(j).add(r);
		    			}
		    		}
		    	}
			}
			page = null;
			System.gc();
    	}
    	
    	
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    		throw new DBAppException();  
    		}
    }
    
    public List<List<Record>>  search2(Object minX, Object maxX, Object minY, Object maxY, Object minZ, Object maxZ) throws DBAppException {
//        List<Object>[] m = new List[2];
        List<List<Record>> res = new ArrayList<List <Record>>();

        for(int j = 0; j<children.length; j++) {
            if( ( compareObjects(minX, children[j].boundsX[2]) < 0 && compareObjects(maxX, children[j].boundsX[1]) >= 0 ) &&
                ( compareObjects(minY, children[j].boundsY[2]) < 0 && compareObjects(maxY, children[j].boundsY[1]) >= 0 ) &&
                ( compareObjects(minZ, children[j].boundsZ[2]) < 0 && compareObjects(maxZ, children[j].boundsZ[1]) >= 0 )) {
                if(children[j].children != null) {
                    res.addAll((ArrayList) children[j].search2(minX, maxX, minY, maxY, minZ, maxZ));
                }
                else {
                    if(children[j].entriesIdPage != null && children[j].entries == null) {
                        children[j].entries = new Vector<Vector<Record>>();
                        this.children[j].deserializeNodeEntries();
                    }
                    for(int i=0; i<children[j].entries.size(); i++) {
                        if(children[j].entries.get(i).size()>0)
                            
                        if( compareObjects(minX, children[j].entries.get(i).get(0).getV().get(boundsX[0])) <= 0 &&
                            compareObjects(maxX, children[j].entries.get(i).get(0).getV().get(boundsX[0])) >= 0 &&
                            compareObjects(minY, children[j].entries.get(i).get(0).getV().get(boundsY[0])) <= 0 &&
                            compareObjects(maxY, children[j].entries.get(i).get(0).getV().get(boundsY[0])) >= 0 &&
                            compareObjects(minZ, children[j].entries.get(i).get(0).getV().get(boundsZ[0])) <= 0 &&
                            compareObjects(maxZ, children[j].entries.get(i).get(0).getV().get(boundsZ[0])) >= 0 ) {
                            res.add(children[j].entries.get(i));
                        }
                    }
                }
            }
        }
        return res;
    }


}
