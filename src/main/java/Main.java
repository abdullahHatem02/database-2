package main.java;
import java.io.*;
import java.util.Vector;

import main.java.Record;

class Example implements Serializable {
    int value;

    public Example(int value, Example ref) {
        this.value = value;
    }
}

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Example y = new Example(2, null);
//        Example x = new Example(1, y);
       Example x = y;

        // Serialize x
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
       ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("test.ser"));
	   out.writeObject(x);
	   y.value =3;
	   
	   ///------------
	   ObjectInputStream in = new ObjectInputStream(new FileInputStream("test.ser"));
		Example r =  (Example) in.readObject();
		
		System.out.println(r.value);
		System.out.println(x.value);
	   

        // Deserialize x
        

        // Verify that deserializedX still references the original y object
//        System.out.println(deserializedX.ref.value); // Output: 2
    }
}
