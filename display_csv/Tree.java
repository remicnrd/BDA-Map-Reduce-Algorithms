package cs.bigdata.Tutorial2;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class Tree {

	public static void main(String[] args) throws IOException {
		
		String localSrc = "Data/arbres.csv";

		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			String line = br.readLine();           // read line by line
			
			while (line !=null){
				String[] col = line.split(";");    // Process of the current line
				if (!col[5].equals("") & !col[6].equals("")){
					System.out.println("Year: "+col[5]+", Height: "+col[6]);
				}
				line = br.readLine();              // go to the next line
			}
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
 
		
		
	}

}
