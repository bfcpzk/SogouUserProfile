package dataprocess;

import com.hankcs.hanlp.HanLP;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * Created by zhaokangpan on 16/10/12.
 */
public class TestKeywordExtraction {

    static Map<String, Map<String, String>> keywordMap = new HashMap<String, Map<String,String>>();

    static Map<String, Set<String>> resultMap = new HashMap<String, Set<String>>();

    public static void main(String[] args) throws IOException{
        File file = new File("train.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file),"utf-8"));
        String line = "";
        Map<String, String> tmp;

        while((line = br.readLine()) != null){
            String[] list = line.split("\t");
            if(keywordMap.get(list[1]) == null)keywordMap.put(list[1], new HashMap<String, String>());
            tmp = keywordMap.get(list[1]);
            String str = "";
            for(int i = 4 ; i < list.length ; i++){
                str += list[i];
            }
            tmp.put(list[0], str);
            keywordMap.put(list[1], tmp);
        }

        for(Object o : keywordMap.keySet()){
            if(resultMap.get(o) == null) resultMap.put(o.toString(), new HashSet<String>());
            Map<String, String> result = keywordMap.get(o);
            for( Object oo : result.keySet()){
                List<String> keywordList = HanLP.extractKeyword(result.get(oo), 500);
                resultMap.get(o).addAll(keywordList);
                //System.out.println(o + "  " + oo + "  " + keywordList);
            }
        }


        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("train_keyword.txt")),"utf-8"));

        for(Object o : resultMap.keySet()){
            System.out.println(o + "  " + resultMap.get(o).toString());
            bw.write(o + "\t" + resultMap.get(o).toString() + "\n");
        }

        br.close();
    }
}
