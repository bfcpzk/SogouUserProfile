package dataprocess;

import java.io.*;

/**
 * Created by zhaokangpan on 16/10/7.
 */
public class Gbk2Utf8 {

    public static void main(String[] args) throws IOException{
        File file = new File("user_tag_query_test.txt");
        FileInputStream fis = new FileInputStream(file);
        InputStreamReader isr = new InputStreamReader(fis,"GBK");
        BufferedReader br = new BufferedReader(isr);
        String line = "";

        File output = new File("test.txt");
        FileOutputStream fos = new FileOutputStream(output);
        OutputStreamWriter osw = new OutputStreamWriter(fos,"utf-8");
        BufferedWriter bw = new BufferedWriter(osw);

        while ((line = br.readLine())!= null){
            //System.out.println(line);
            bw.write(line + "\n");
        }
        bw.close();
        osw.close();
        fos.close();

        br.close();
        isr.close();
        fis.close();
    }
}
