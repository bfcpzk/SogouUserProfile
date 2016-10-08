package dataprocess;

import java.io.*;

/**
 * Created by zhaokangpan on 16/10/7.
 */
public class DataSuitWeka {

    public static void main(String[] args) throws IOException{
        File file = new File("test_theta_output.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "utf-8"));
        String line = "";

        File out = new File("test_theta_output_weka.csv");
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out), "utf-8"));

        br.readLine();

        while((line = br.readLine()) != null){
            /*String[] list = line.split(",");
            list[16] = "s" + list[16];
            list[17] = "s" + list[17];
            list[18] = "s" + list[18];
            for(int i = 0 ; i < list.length - 1; i++){
                bw.write(list[i] + ",");
            }
            bw.write(list[list.length - 1] + "\n");*/
            bw.write(line + ",a,g,e\n");
        }

        br.close();
        bw.close();
    }
}
