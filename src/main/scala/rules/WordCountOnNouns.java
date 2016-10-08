package rules;

import util.Jdbc_Util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhaokangpan on 16/10/8.
 */
public class WordCountOnNouns {
    public static void main(String[] args) throws IOException{

        Jdbc_Util db = new Jdbc_Util();
        String sql = "";

        File file = new File("train_nouns.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "utf-8"));
        String line = "";

        Map<Integer, Map<String, Integer>> resultMap = new HashMap<Integer, Map<String, Integer>>();
        resultMap.put(0, new HashMap<String, Integer>());
        resultMap.put(1, new HashMap<String, Integer>());
        resultMap.put(2, new HashMap<String, Integer>());
        resultMap.put(3, new HashMap<String, Integer>());
        resultMap.put(4, new HashMap<String, Integer>());
        resultMap.put(5, new HashMap<String, Integer>());
        resultMap.put(6, new HashMap<String, Integer>());

        while((line = br.readLine())!=null){
            String[] dict = line.split("\t");
            if(dict.length == 5){
                String[] wordlist = dict[4].split(" ");
                for(int i = 0 ; i < wordlist.length ; i++){
                    if(resultMap.get(Integer.parseInt(dict[1])).containsKey(wordlist[i])){
                        int tmp = resultMap.get(Integer.parseInt(dict[1])).get(wordlist[i]) + 1;
                        resultMap.get(Integer.parseInt(dict[1])).put(wordlist[i], tmp);
                    }else{
                        resultMap.get(Integer.parseInt(dict[1])).put(wordlist[i], 1);
                    }
                }
            }
        }

        //将统计结果插入数据库
        for(Object t : resultMap.keySet()){
            Map<String, Integer> temp = resultMap.get(t);
            for(Object w : temp.keySet()){
                sql = "insert into `s_age_word_count` (`age`, `word`, `wordcount`) VALUES ('" + t + "', '" + w + "', '" + temp.get(w) + "')";
                db.add(sql);
            }
        }
    }
}
