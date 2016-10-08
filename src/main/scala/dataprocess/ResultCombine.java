package dataprocess;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by zhaokangpan on 16/10/8.
 */
public class ResultCombine {
    public static void main(String[] args) throws IOException{

        File out = new File("allresult.csv");
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out),"GBK"));

        File file = new File("results.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file),"utf-8"));
        String line = "";

        File all = new File("test_nouns.txt");
        BufferedReader bra = new BufferedReader(new InputStreamReader(new FileInputStream(all),"utf-8"));

        Set<String> hasResultSet = new HashSet<String>();
        List<String> allSet = new ArrayList<String>();

        while((line = br.readLine()) != null){
            hasResultSet.add(line.split(" ")[0]);
            bw.write(line + "\n");
        }

        while((line = bra.readLine()) != null){
            allSet.add(line.split("\t")[0]);
        }

        Set<String> sub = new HashSet<String>();

        for(int i = 0 ; i < allSet.size() ; i++){
            if(hasResultSet.contains(allSet.get(i))){
                continue;
            }else{
                sub.add(allSet.get(i));
            }
        }

        for(String i : sub){
            bw.write(i + " 1 1 5\n");
        }
        bw.close();
        br.close();
        bra.close();
    }
}
