package cluster.lda.LDA4J;

import java.io.*;
import java.util.Map;



public class ldatest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		// 1. Load corpus from disk
		//Corpus corpus = Corpus.load_file("train_nouns.txt", 4);
		Corpus corpus = Corpus.load_file("test_nouns.txt", 1);
		//Corpus corpus = Corpus.load("data/mini");
		// 2. Create a LDA sampler
		LdaGibbsSampler ldaGibbsSampler = new LdaGibbsSampler(corpus.getDocument(), corpus.getVocabularySize());
		// 3. Train it
		ldaGibbsSampler.gibbs(15);
		// 4. The phi matrix is a LDA model, you can use LdaUtil to explain it.
		double[][] phi = ldaGibbsSampler.getPhi();
		Map<String, Double>[] topicMap = LdaUtil.translate(phi, corpus.getVocabulary(), 10);
		LdaUtil.explain(topicMap);
		double[][] theta = ldaGibbsSampler.getTheta();

		/*File file = new File("train_nouns.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));

		File output = new File("train_theta_output.csv");
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output), "UTF-8"));
		for(int i = 0 ; i < theta.length ; i++){
			String line = br.readLine();
			String[] list = line.split("\t");
			if(list.length == 5){
				bw.write(list[0] + ",");
				for(int j = 0 ; j < theta[i].length ; j++){
					bw.write(theta[i][j] + ",");
				}
				bw.write(list[1] + "," + list[2] + "," + list[3] + "\n");
			}
		}
		br.close();
		bw.close();*/
		File file = new File("test_nouns.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));

		File output = new File("test_theta_output.csv");
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output), "UTF-8"));
		for(int i = 0 ; i < theta.length ; i++){
			String line = br.readLine();
			String[] list = line.split("\t");
			if(list.length == 2){
				bw.write(list[0] + ",");
				for(int j = 0 ; j < theta[i].length - 1 ; j++){
					bw.write(theta[i][j] + ",");
				}
				bw.write(theta[i][theta[i].length - 1] + "\n");
			}
		}
	}
}