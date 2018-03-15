package cs.bigdata.Tutorial2;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class Station {

	public static void main(String[] args) throws IOException {
		
		String localSrc = "isd-history.txt";

		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);

			int i = 0;
			
			String line = br.readLine();           // read line by line
			
			while (line !=null){
				i += 1;                            // Process of the current line
				if (i > 22){
					String usaf = line.substring(0, 6);
					String name = line.substring(13, 42);
					String country = line.substring(43, 45);
					String elevation = line.substring(74, 81);
					System.out.println(usaf + "; " + name + "; " + country + "; " + elevation);
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
