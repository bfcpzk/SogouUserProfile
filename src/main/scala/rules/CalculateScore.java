package rules;

import model.*;
import util.Jdbc_Util;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by zhaokangpan on 16/10/9.
 */
public class CalculateScore {

    static Map<String, Age> ageMap = new HashMap<String, Age>();
    static Map<String, Education> educationMap = new HashMap<String, Education>();
    static Map<String, Gender> genderMap = new HashMap<String, Gender>();
    static List<Label> testData = new ArrayList<Label>();

    public static void main(String[] args) throws SQLException,IOException{

        File file = new File("test.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file),"utf-8"));
        String line = "";

        Jdbc_Util db = new Jdbc_Util();
        String sql = "";

        //gender
        sql = "select * from s_gender_word_count";
        ResultSet rs;
        rs = db.select(sql);

        //初始化性别内存词典对象
        while(rs.next()){
            if(genderMap.get(rs.getString("word")) != null){
                if(rs.getInt("gender") == 1){
                    genderMap.get(rs.getString("word")).setOne(rs.getDouble("count"));
                }else if(rs.getInt("gender") == 2){
                    genderMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }
            }else{
                genderMap.put(rs.getString("word"), new Gender());
                if(rs.getInt("gender") == 1){
                    genderMap.get(rs.getString("word")).setOne(rs.getDouble("count"));
                }else if(rs.getInt("gender") == 2){
                    genderMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }
            }
        }

        //初始化年龄内存词典对象
        while(rs.next()){
            if(ageMap.get(rs.getString("word")) != null){
                if(rs.getInt("age") == 1){
                    ageMap.get(rs.getString("word")).setOne(rs.getDouble("count"));
                }else if(rs.getInt("age") == 2){
                    ageMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("age") == 3){
                    ageMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("age") == 4){
                    ageMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("age") == 5){
                    ageMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("age") == 6){
                    ageMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }
            }else{
                ageMap.put(rs.getString("word"), new Age());
                if(rs.getInt("age") == 1){
                    ageMap.get(rs.getString("word")).setOne(rs.getDouble("count"));
                }else if(rs.getInt("age") == 2){
                    ageMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("age") == 3){
                    ageMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("age") == 4){
                    ageMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("age") == 5){
                    ageMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("age") == 6){
                    ageMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }
            }
        }

        //初始化教育内存词典对象
        while(rs.next()){
            if(educationMap.get(rs.getString("word")) != null){
                if(rs.getInt("education") == 1){
                    educationMap.get(rs.getString("word")).setOne(rs.getDouble("count"));
                }else if(rs.getInt("education") == 2){
                    educationMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("education") == 3){
                    educationMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("education") == 4){
                    educationMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("education") == 5){
                    educationMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("education") == 6){
                    educationMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }
            }else{
                educationMap.put(rs.getString("word"), new Education());
                if(rs.getInt("education") == 1){
                    educationMap.get(rs.getString("word")).setOne(rs.getDouble("count"));
                }else if(rs.getInt("education") == 2){
                    educationMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("education") == 3){
                    educationMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("education") == 4){
                    educationMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("education") == 5){
                    educationMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }else if(rs.getInt("education") == 6){
                    educationMap.get(rs.getString("word")).setTwo(rs.getDouble("count"));
                }
            }
        }


        while((line = br.readLine()) != null){
            String[] list = line.split("\t");
            String[] wlist = list[1].split(" ");
            Label l = new Label();
            l.setId(list[0]);
            for(int i = 0 ; i < wlist.length ; i++){
                if(genderMap.get(wlist[i]) == null) continue;
                else{
                    Gender gender = genderMap.get(wlist[i]);
                    l.getGender().setOne(gender.getOne() + l.getGender().getOne());
                    l.getGender().setTwo(gender.getTwo() + l.getGender().getTwo());
                }
                if(ageMap.get(wlist[i]) == null) continue;
                else{
                    Age age = ageMap.get(wlist[i]);
                    l.getAge().setOne(age.getOne() + l.getAge().getOne());
                    l.getAge().setTwo(age.getTwo() + l.getAge().getTwo());
                    l.getAge().setThree(age.getThree() + l.getAge().getThree());
                    l.getAge().setFour(age.getFour() + l.getAge().getFour());
                    l.getAge().setFive(age.getFive() + l.getAge().getFive());
                    l.getAge().setSix(age.getSix() + l.getAge().getSix());
                }
                if(educationMap.get(wlist[i]) == null) continue;
                else{
                    Education education = educationMap.get(wlist[i]);
                    l.getEducation().setOne(education.getOne() + l.getEducation().getOne());
                    l.getEducation().setTwo(education.getTwo() + l.getEducation().getTwo());
                    l.getEducation().setThree(education.getThree() + l.getEducation().getThree());
                    l.getEducation().setFour(education.getFour() + l.getEducation().getFour());
                    l.getEducation().setFive(education.getFive() + l.getEducation().getFive());
                    l.getEducation().setSix(education.getSix() + l.getEducation().getSix());
                }
            }
            testData.add(l);
        }

        br.close();

        Random rand = new Random();

        List<Result> rlist = new ArrayList<Result>();
        for(int i = 0 ; i < testData.size() ; i++){
            Result result = new Result();
            result.setId(testData.get(i).getId());
            if(testData.get(i).getGender().getOne() > testData.get(i).getGender().getTwo()){
                result.setGender("1");
            }else if(testData.get(i).getGender().getOne() < testData.get(i).getGender().getTwo()){
                result.setGender("2");
            }else{
                result.setGender(String.valueOf(rand.nextInt(2) + 1));
            }

            double ageMax = 0.0;
            int flag = 0;
            if(testData.get(i).getAge().getOne() > ageMax){
                ageMax = testData.get(i).getAge().getOne();
                flag = 1;
            }
            if(testData.get(i).getAge().getTwo() > ageMax){
                ageMax = testData.get(i).getAge().getTwo();
                flag = 2;
            }
            if(testData.get(i).getAge().getThree() > ageMax){
                ageMax = testData.get(i).getAge().getThree();
                flag = 3;
            }
            if(testData.get(i).getAge().getFour() > ageMax){
                ageMax = testData.get(i).getAge().getFour();
                flag = 4;
            }
            if(testData.get(i).getAge().getFive() > ageMax){
                ageMax = testData.get(i).getAge().getFive();
                flag = 5;
            }
            if(testData.get(i).getAge().getSix() > ageMax){
                ageMax = testData.get(i).getAge().getSix();
                flag = 6;
            }
            if(ageMax == 0.0){
                flag = rand.nextInt(6) + 1;
            }
            result.setAge(String.valueOf(flag));

            double eduMax = 0.0;
            flag = 0;
            if(testData.get(i).getEducation().getOne() > eduMax){
                eduMax = testData.get(i).getEducation().getOne();
                flag = 1;
            }
            if(testData.get(i).getEducation().getTwo() > eduMax){
                eduMax = testData.get(i).getEducation().getTwo();
                flag = 2;
            }
            if(testData.get(i).getEducation().getThree() > eduMax){
                eduMax = testData.get(i).getEducation().getThree();
                flag = 3;
            }
            if(testData.get(i).getEducation().getFour() > eduMax){
                eduMax = testData.get(i).getEducation().getFour();
                flag = 4;
            }
            if(testData.get(i).getEducation().getFive() > eduMax){
                eduMax = testData.get(i).getEducation().getFive();
                flag = 5;
            }
            if(testData.get(i).getEducation().getSix() > eduMax){
                eduMax = testData.get(i).getEducation().getSix();
                flag = 6;
            }
            if(eduMax == 0.0){
                flag = rand.nextInt(6) + 1;
            }
            result.setEducation(String.valueOf(flag));

            rlist.add(result);
        }

        File output = new File("test_regulation_simple.txt");
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output),"GBK"));
        for(int i = 0 ; i < rlist.size() ; i++){
            bw.write(rlist.get(i).getId() + " " + rlist.get(i).getAge() + " " + rlist.get(i).getGender() + " " + rlist.get(i).getEducation() + "\n");
        }

        bw.close();
    }
}
