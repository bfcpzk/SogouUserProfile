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

        Map<String , Integer> allMap = new HashMap<String, Integer>();
        Map<Integer, Map<String, Double>> resultMap = new HashMap<Integer, Map<String, Double>>();
        resultMap.put(1, new HashMap<String, Double>());
        resultMap.put(2, new HashMap<String, Double>());
//        resultMap.put(3, new HashMap<String, Double>());
//        resultMap.put(4, new HashMap<String, Double>());
//        resultMap.put(5, new HashMap<String, Double>());
//        resultMap.put(6, new HashMap<String, Double>());

        while((line = br.readLine())!=null){
            String[] dict = line.split("\t");
            if(dict.length == 5 && !dict[2].equals("0")){
                String[] wordlist = dict[4].split(" ");
                for(int i = 0 ; i < wordlist.length ; i++){
                    if(allMap.containsKey(wordlist[i])){
                        int tmp = allMap.get(wordlist[i]) + 1;
                        allMap.put(wordlist[i], tmp);
                    }else{
                        allMap.put(wordlist[i], 1);
                    }

                    if(resultMap.get(Integer.parseInt(dict[2])).containsKey(wordlist[i])){
                        double tmp = resultMap.get(Integer.parseInt(dict[2])).get(wordlist[i]) + 1;
                        resultMap.get(Integer.parseInt(dict[2])).put(wordlist[i], tmp);
                    }else{
                        resultMap.get(Integer.parseInt(dict[2])).put(wordlist[i], 1.0);
                    }
                }
            }
        }

        for(Object t : resultMap.keySet()){
            Map<String, Double> temp = resultMap.get(t);
            for(Object w : temp.keySet()){
                resultMap.get(t).put(w.toString(), temp.get(w)/allMap.get(w));
            }
        }

        //将统计结果插入数据库
        for(Object t : resultMap.keySet()){
            Map<String, Double> temp = resultMap.get(t);
            for(Object w : temp.keySet()){
                sql = "insert into `s_gender_word_count` (`gender`, `word`, `count`) VALUES ('" + t + "', '" + w + "', '" + temp.get(w) + "')";
                db.add(sql);
            }
        }
    }
}
