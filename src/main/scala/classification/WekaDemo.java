package classification;

import model.Result;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Attribute;
import weka.core.Instances;
import weka.core.converters.ConverterUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.*;

/**
 * Created by zhaokangpan on 16/10/8.
 */
public class WekaDemo {

    private static List<Result> result = new ArrayList<Result>();


    public static void main(String[] args) throws Exception{

        //加载数据
        Instances data = ConverterUtils.DataSource.read("train_theta_output_weka.csv");
        Instances ageData = new Instances(data);
        Instances educationData = new Instances(data);
        Instances testData = ConverterUtils.DataSource.read("test_theta_output_weka.csv");
        Instances educationTestData = new Instances(testData);
        Instances ageTestData = new Instances(testData);


        System.out.println("testData:" + testData.size());
        System.out.println("ageTestData:" + ageTestData.size());
        System.out.println("educationTestData:" + educationTestData.size());

        List<String> idlist = new ArrayList<String>();
        for(int i = 0 ; i < testData.numInstances(); i++){
            idlist.add(testData.get(i).stringValue(testData.attribute(0)));
        }
        System.out.println("idlist:" + idlist.size());

        //过滤数据
        Set<String> filterSet = new HashSet<String>();
        filterSet.add("id");
        filterSet.add("sage");
        filterSet.add("seducation");
        List<Integer> removeAttrIndex = new ArrayList<Integer>();
        for(int i = 0 ; i < data.numAttributes() ; i++){
            if(filterSet.contains(data.attribute(i).name())){
                removeAttrIndex.add(i);
            }
        }
        for(int i = 0 ; i < removeAttrIndex.size() ; i++){
            data.deleteAttributeAt(removeAttrIndex.get(i) - i);
            testData.deleteAttributeAt(removeAttrIndex.get(i) - i);
        }

        filterSet.removeAll(filterSet);
        filterSet.add("id");
        filterSet.add("sgender");
        filterSet.add("seducation");
        removeAttrIndex.removeAll(removeAttrIndex);
        for(int i = 0 ; i < ageData.numAttributes() ; i++){
            if(filterSet.contains(ageData.attribute(i).name())){
                removeAttrIndex.add(i);
            }
        }
        for(int i = 0 ; i < removeAttrIndex.size() ; i++){
            ageData.deleteAttributeAt(removeAttrIndex.get(i) - i);
            ageTestData.deleteAttributeAt(removeAttrIndex.get(i) - i);
        }

        filterSet.removeAll(filterSet);
        filterSet.add("id");
        filterSet.add("sgender");
        filterSet.add("sage");
        removeAttrIndex.removeAll(removeAttrIndex);
        for(int i = 0 ; i < educationData.numAttributes() ; i++){
            if(filterSet.contains(educationData.attribute(i).name())){
                removeAttrIndex.add(i);
            }
        }
        for(int i = 0 ; i < removeAttrIndex.size() ; i++){
            educationData.deleteAttributeAt(removeAttrIndex.get(i) - i);
            educationTestData.deleteAttributeAt(removeAttrIndex.get(i) - i);
        }

        System.out.println("testData:" + testData.size());
        System.out.println("ageTestData:" + ageTestData.size());
        System.out.println("educationTestData:" + educationTestData.size());

        //设置类别索引
        data.setClassIndex(data.numAttributes() - 1);
        testData.setClassIndex(testData.numAttributes() - 1);
        ageData.setClassIndex(ageData.numAttributes() - 1);
        ageTestData.setClassIndex(ageTestData.numAttributes() - 1);
        educationData.setClassIndex(educationData.numAttributes() - 1);
        educationTestData.setClassIndex(educationTestData.numAttributes() - 1);

        //分类器
        NaiveBayes classifier = new NaiveBayes();
        NaiveBayes classifier_a = new NaiveBayes();
        NaiveBayes classifier_e = new NaiveBayes();
        classifier.buildClassifier(data);
        classifier_a.buildClassifier(ageData);
        classifier_e.buildClassifier(educationData);

        //其他选项
        /*int seed = 1234;//随机种子
        int folds = 10;//折数

        //随机化数据
        Random rand = new Random(seed);
        Instances newData = new Instances(data);
        newData.randomize(rand);

        if(newData.classAttribute().isNominal()){
            newData.stratify(folds);
        }*/

        for(int j = 0 ; j < testData.numInstances(); j++){
            double clsLabel = classifier.classifyInstance(testData.instance(j));
            double clsLabel_a = classifier_a.classifyInstance(ageTestData.instance(j));
            double clsLabel_e = classifier_e.classifyInstance(educationTestData.instance(j));
            //System.out.println(idlist.get(j) + "    " + data.attribute(data.numAttributes() - 1).value((int)clsLabel) + "    " + ageData.attribute(ageData.numAttributes() - 1).value((int)clsLabel_a) + "    " + educationData.attribute(educationData.numAttributes() - 1).value((int)clsLabel_e));
            Result re = new Result();
            re.setId(idlist.get(j));
            re.setAge(ageData.attribute(ageData.numAttributes() - 1).value((int)clsLabel_a).replaceAll("s",""));
            re.setGender(data.attribute(data.numAttributes() - 1).value((int)clsLabel).replaceAll("s",""));
            re.setEducation(educationData.attribute(educationData.numAttributes() - 1).value((int)clsLabel_e).replaceAll("s",""));
            result.add(re);
        }
        File file = new File("results.csv");
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));

        for(int i = 0 ; i < result.size() ; i++){
            //System.out.println(result.get(i));
            bw.write(result.get(i).getId() + " " + result.get(i).getAge() + " " + result.get(i).getGender() + " " + result.get(i).getEducation() + "\n");
        }

        bw.close();
        //执行交叉验证
        /*Evaluation eval = new Evaluation(newData);
        for(int i = 0 ; i < folds ; i++){
            //训练集
            Instances train = newData.trainCV(10, i);
            //测试集
            Instances test = newData.testCV(10, i);
            //构建评估分类器
            classifier.buildClassifier(train);
            eval.evaluateModel(classifier, test);

        }*/
    }
}