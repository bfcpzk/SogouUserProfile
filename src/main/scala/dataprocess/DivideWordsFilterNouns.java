package dataprocess;

import org.ansj.splitWord.analysis.ToAnalysis;

import java.io.*;

/**
 * Created by zhaokangpan on 16/10/7.
 */
public class DivideWordsFilterNouns {

    public static void main(String[] args) throws IOException{
        File file = new File("test.txt");
        FileInputStream fis = new FileInputStream(file);
        InputStreamReader isr = new InputStreamReader(fis,"utf-8");
        BufferedReader br = new BufferedReader(isr);
        String line = "";

        File output = new File("test_nouns.txt");
        FileOutputStream fos = new FileOutputStream(output);
        OutputStreamWriter osw = new OutputStreamWriter(fos,"utf-8");
        BufferedWriter bw = new BufferedWriter(osw);

        while((line = br.readLine())!= null){
            String[] taglist = line.split("\t");
            String nouns = "";
            for(int i = 1 ; i < taglist.length ; i++){
                taglist[i] = ToAnalysis.parse(taglist[i]).toString();
                String[] wordlist = taglist[i].split(",");
                for(int j = 0 ; j < wordlist.length ; j++){
                    if(wordlist[j].contains("/n")){//筛选名词
                        nouns += wordlist[j].split("/")[0] + " ";
                    }
                }
            }
            bw.write(taglist[0] + "\t" + nouns + "\n");
        }

        bw.close();
        osw.close();
        fos.close();

        br.close();
        isr.close();
        fis.close();
    }
}
