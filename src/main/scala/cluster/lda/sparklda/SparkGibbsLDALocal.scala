package cluster

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.Random

/**
  * Implement the gibbs sampling LDA on spark
  * Input file's format is: docId \t date \t words(splited by " ")
  * Output the topic distribution of each file in "out/topicDistOnDoc" default
  * and the topic in "out/wordDistOnTopic" default
  *
  * huangwaleking@gmail.com
  * liupeng9966@163.com
  * 2013-04-24
  */
object SparkGibbsLDALocal {

  /**
    * print out topics
    * output topK words in each topic
    */
  def topicsInfo(nkv: Array[Array[Int]], allWords: List[String], kTopic: Int, vSize: Int, topK: Int) = {
    var res = ""
    for (k <- 0 until kTopic) {
      val distOnTopic = for (v <- 0 until vSize) yield (v, nkv(k)(v))
      val sorted = distOnTopic.sortWith((tupleA, tupleB) => tupleA._2 > tupleB._2)
      res = res + "topic " + k + ":" + "\n"
      for (j <- 0 until topK) {
        res = res + "n(" + allWords(sorted(j)._1) + ")=" + sorted(j)._2 + " "
      }
      res = res + "\n"
    }
    res
  }
  /**
    * gibbs sampling
    * topicAssignArr Array[(word,topic)]
    * nmk: Array[n_{mk}]
    */
  def gibbsSampling(topicAssignArr: Array[(Int, Int)],
                    nmk: Array[Int], nkv: Array[Array[Int]], nk: Array[Int],
                    kTopic: Int, alpha: Double, vSize: Int, beta: Double) = {//采样对一篇文档进行的
  val length = topicAssignArr.length
    for (i <- 0 until length) {//对于该篇文档中的每一个词,进行一次更新,nmk,nk,nkv
    val topic = topicAssignArr(i)._2
      val word = topicAssignArr(i)._1
      //reset nkv,nk and nmk
      nmk(topic) = nmk(topic) - 1
      nkv(topic)(word) = nkv(topic)(word) - 1
      nk(topic) = nk(topic) - 1
      //sampling
      val topicDist = new Array[Double](kTopic) //Important, not Array[Double](kTopic) which will lead to Array(4.0)
      for (k <- 0 until kTopic) {
        nmk(k)
        nkv(k)(word)
        nk(k)
        topicDist(k) = (nmk(k).toDouble + alpha) * (nkv(k)(word) + beta) / (nk(k) + vSize * beta)
      }
      val newTopic = getRandFromMultinomial(topicDist)
      topicAssignArr(i) = (word, newTopic) //Important, not (newTopic,word)
      //update nkv,nk and nmk locally
      nmk(newTopic) = nmk(newTopic) + 1
      nkv(newTopic)(word) = nkv(newTopic)(word) + 1
      nk(newTopic) = nk(newTopic) + 1
    }
    (topicAssignArr, nmk)
  }

  // get nkv matrix
  //List(((0,0),2), ((0,1),1),((word,topic),count))
  //=> Array[Array(...)]
  def updateNKV(wordsTopicReduced: List[((Int, Int), Int)], kTopic: Int, vSize: Int) = {
    val nkv = new Array[Array[Int]](kTopic)
    for (k <- 0 until kTopic) {
      nkv(k) = new Array[Int](vSize)
    }
    wordsTopicReduced.foreach(t => { //t is ((Int,Int),Int) which is ((word,topic),count)
    val word = t._1._1
      val topic = t._1._2
      val count = t._2
      nkv(topic)(word) = nkv(topic)(word) + count
    })
    nkv//记录了每个主题下每个词对应的个数(单词 t 属于主题 k 的数量)
  }

  //get nk vector
  //List(((0,0),2), ((0,1),1),((word,topic),count))
  //=> Array[Array(...)]
  def updateNK(wordsTopicReduced: List[((Int, Int), Int)], kTopic: Int, vSize: Int) = {
    val nk = new Array[Int](kTopic)
    wordsTopicReduced.foreach(t => { //t is ((Int,Int),Int) which is ((word,topic),count)
    val topic = t._1._2
      val count = t._2
      nk(topic) = nk(topic) + count
    })
    nk//记录了每个主题 k 下单词总数
  }

  /**
    *  get a topic from Multinomial Distribution
    *  usage example: k=getRand(Array(0.1, 0.2, 0.3, 1.1)),
    */
  def getRandFromMultinomial(arrInput: Array[Double]): Int = {
    val rand = Random.nextDouble()
    val s = doubleArrayOps(arrInput).sum
    val arrNormalized = doubleArrayOps(arrInput).map { e => e / s }
    var localsum = 0.0
    val cumArr = doubleArrayOps(arrNormalized).map { dist =>
      localsum = localsum + dist
      localsum
    }
    //return the new topic
    doubleArrayOps(cumArr).indexWhere(cumDist => cumDist >= rand)
  }

  def restartSpark(sc: SparkContext, scMaster: String, remote: Boolean): SparkContext = {
    // After iterations, Spark will create a lot of RDDs and I only have 4g mem for it.
    // So I have to restart the Spark. The thread.sleep is for the shutting down of Akka.
    sc.stop()
    Thread.sleep(2000)
    if (remote == true) {
      new SparkContext(scMaster, "SparkGibbsLDAWeibo", "./", Seq("SparkGibbsLDAWeibo.jar"))
    } else {
      new SparkContext(scMaster, "SparkGibbsLDA")
    }
  }

  /**
    * start spark at 192.9.200.175:7077 if remote==true
    * or start it locally when remote==false
    */
  def startSpark(remote: Boolean) = {
    if (remote == true) {
      val scMaster = "spark://202.112.113.199:7077" // e.g. 集群
      val sparkContext = new SparkContext(scMaster, "SparkGibbsLDAWeibo", "./", Seq("SparkGibbsLDAWeibo.jar"))
      (scMaster, sparkContext)
    } else {
      val scMaster = "local[4]" // e.g. local[4]
      val sparkContext = new SparkContext(scMaster, "SparkGibbsLDA")
      (scMaster, sparkContext)
    }
  }

  /**
    * save topic distribution of doc in HDFS
    * INPUT: doucments which is RDD[(docId,topicAssigments,nmk)]
    * format: docID, topic distribution
    */
  def saveDocTopicDist(documents: RDD[(String, String, String, String, Array[(Int, Int)], Array[Int])], pathTopicDistOnDoc: String): Any = {
    documents.map {
      case (docId, age, gender, education, topicAssign, nmk) =>
        val docLen = topicAssign.length
        val probabilities = nmk.map(n => n / docLen.toDouble).toList
        var pstr = ""
        for( p <- probabilities){
          pstr = pstr + p + " "
        }
        val res = docId + ", " + age + "," + gender + "," + education + "," + probabilities
        res
    }.saveAsTextFile(pathTopicDistOnDoc)
    documents.unpersist(blocking = false)
  }

  /**
    * save word distribution on topic into HDFS
    * output format:
    * (topicID,List((#/x,0.05803571428571429)...(与/p,0.04017857142857143),...))
    *
    */
  def saveWordDistTopic(sc: SparkContext, nkv: Array[Array[Int]], nk: Array[Int],
                        allWords: List[String], vSize: Int, topKwordsForDebug: Int, pathWordDistOnTopic: String) {
    val topicK = nkv.length
    //add topicid for array
    val nkvWithId = Array.fill(topicK) { (0, Array[Int](vSize)) }
    for (k <- 0 until topicK) {
      nkvWithId(k) = (k, nkv(k))
    }
    //output topKwordsForDebug words
    val res = sc.parallelize(nkvWithId).map { t => //topicId, Array(2,3,3,4,...)
      {
        val topDist = { for (v <- 0 until t._2.length) yield (t._2(v).toDouble / nk(t._1).toDouble) }.toList
        t._1 + ", " + topDist
      }
    }

    res.saveAsTextFile(pathWordDistOnTopic)
    res.unpersist(blocking = false)
  }

  /**
    *  save nkv, nk, nmk in each doc
    */
  def saveNkvNkNmk(sc: SparkContext, vSize : Int, nkv: Array[Array[Int]], nk: Array[Int], nmk : Array[(String, String, Array[Int])]): Unit ={
    val topicK = nkv.length
    //save nkv
    val nkvWithId = Array.fill(topicK) { (0, Array[Int](vSize)) }
    for (k <- 0 until topicK) {
      nkvWithId(k) = (k, nkv(k))
    }
    sc.parallelize(nkvWithId).map{
      case(k, nkvtmp) =>
        k + ", " + nkvtmp.toList
    }.saveAsTextFile("/Users/zhaokangpan/Documents/sparklda/weibo/out/nkv")

    //val sc1 = restartSpark(sc, "spark://202.112.113.199:7077", true)

    //save nk
    val nkWithId = new ArrayBuffer[(Int, Int)]()
    for (k <- 0 until topicK) {
      nkWithId.+=((k, nk(k)))
    }
    sc.parallelize(nkWithId).map( l => l._1 + " " + l._2).saveAsTextFile("/Users/zhaokangpan/Documents/sparklda/weibo/out/nk")


    //save nmk
    sc.parallelize(nmk).map{
      case(userId, docId, nmk) =>
        val nmklist = nmk.toList
        userId + ", " + docId + ", " + nmklist
    }.saveAsTextFile("/Users/zhaokangpan/Documents/sparklda/weibo/out/nmk")
  }

  def saveWordIndexMap( sc: SparkContext, wordMap : HashMap[String, Int]): Unit ={
    val wordArray = new ArrayBuffer[String]()
    for(item <- wordMap){
      wordArray.+=(item._2 + " " + item._1)
    }
    sc.parallelize(wordArray).saveAsTextFile("/Users/zhaokangpan/IDEA/SogouUserProfile/wordMap")
  }


  /**
    * the lda's executing function
    * do the following things:
    * 1,start spark
    * 2,read files into HDFS
    * 3,build a dictionary for alphabet : wordIndexMap
    * 4,init topic assignments for each word in the corpus
    * 5,use gibbs sampling to infer the topic distribution of doc and estimate the parameter nkv and nk
    * 6,save the result in HDFS (result part 1: topic distribution of doc, result part 2: top words in each topic)
    */
  def lda(filename: String, kTopic: Int, alpha: Double, beta: Double,
          maxIter: Int, remote: Boolean, topKwordsForDebug: Int,
          pathTopicDistOnDoc: String, pathWordDistOnTopic: String, iterflag: Int, rate: Double) {
    //Step 1, start spark
    System.setProperty("file.encoding", "UTF-8")
    var (scMaster, sc) = startSpark(remote)

    //Step2, read files into HDFS
    val tempFile = sc.textFile(filename).filter{
      line => line.split("\t").length == 5
    }.coalesce(10, false)

    val rawFiles = tempFile.map { line =>
      {
        val vs = line.split("\t")
        val words = vs(4).split(" ").toList
        //println(words)
        (vs(0), vs(1), vs(2), vs(3), words)
      }
    }
    //释放资源
    tempFile.unpersist(blocking = false)

    //Step3, build a dictionary for alphabet : wordIndexMap
    val allWords = rawFiles.flatMap { t =>
      t._5.distinct
    }.map{ t => (t,1) }.reduceByKey(_+_).map{_._1}.collect().toList.sortWith(_ < _)
    val vSize = allWords.length
    //println(allWords)
    var wordIndexMap = new HashMap[String, Int]()
    for (i <- 0 until allWords.length) {
      wordIndexMap(allWords(i)) = i
    }
    val bWordIndexMap = wordIndexMap
    saveWordIndexMap(sc, bWordIndexMap)

    //Step4, init topic assignments for each word in the corpus
    var documents = rawFiles.map { t => //t means (docId,words) where words is a List
      val docId = t._1
      val length = t._5.length
      val topicAssignArr = new Array[(Int, Int)](length)
      val nmk = new Array[Int](kTopic)
      for (i <- 0 until length) {
        val topic = Random.nextInt(kTopic)//随机生成一个主题编号
        topicAssignArr(i) = (bWordIndexMap(t._5(i)), topic) //((word1,topic1),(word2,topic2),...)长度为doc中的所有词
        nmk(topic) = nmk(topic) + 1 //doc中包含topic的次数
      }
      (docId, t._2, t._3, t._4, topicAssignArr, nmk) //t._1 means userId, t._2 means docId, t._3 means words
    }.cache

    //资源回收
    rawFiles.unpersist(blocking = false)
    //当前topic k 下的word t 数量
    var wordsTopicReduced = documents.flatMap(t => t._5).map(t => (t, 1)).reduceByKey(_ + _).collect().toList
    //update nkv,nk
    var nkv = updateNKV(wordsTopicReduced, kTopic, vSize)
    var nk = updateNK(wordsTopicReduced, kTopic, vSize)
    //nk.foreach(println)

    //Step5, use gibbs sampling to infer the topic distribution in doc and estimate the parameter nkv and nk
    var iterativeInputDocuments = documents
    var updatedDocuments = iterativeInputDocuments
    updatedDocuments.unpersist(blocking = false)
    documents.unpersist(blocking = false)
    for (iter <- 1 until maxIter + 1) {
      //iterativeInputDocuments.persist(StorageLevel.MEMORY_ONLY)//same as cache

      //broadcast the global data
      //var nkvGlobal = sc.broadcast(nkv)
      //var nkGlobal = sc.broadcast(nk)

      updatedDocuments = iterativeInputDocuments.map {
        case (docId, age, gender, education, topicAssignArr, nmk) =>
          //gibbs sampling
          val (newTopicAssignArr, newNmk) = gibbsSampling(topicAssignArr,
            nmk, nkv, nk,
            kTopic, alpha, vSize, beta)
          (docId, age, gender, education, newTopicAssignArr, newNmk)
      }

      //nkGlobal.unpersist(blocking = false)
      //nkvGlobal.unpersist(blocking = false)
      wordsTopicReduced = updatedDocuments.flatMap(t => t._5).map(t => (t, 1)).reduceByKey(_ + _).collect().toList
      iterativeInputDocuments.unpersist(blocking = false)
      iterativeInputDocuments = updatedDocuments
      updatedDocuments.unpersist(blocking = false)
      //update nkv,nk

      nkv = updateNKV(wordsTopicReduced, kTopic, vSize)
      nk = updateNK(wordsTopicReduced, kTopic, vSize)

      println("iteration " + iter + " finished")

      //restart spark to optimize the memory
      if (iter % iterflag == 0) {
        //save RDD temporally
        var pathDocument1=""
        var pathDocument2=""
        if(remote==true){
          pathDocument1="hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/out/gibbsLDAtmp_final_" + kTopic + "_" + iter
          pathDocument2="hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/out/gibbsLDAtmp2_final_" + kTopic + "_" + iter
        }else{
          pathDocument1="/Users/zhaokangpan/IDEA/SogouUserProfile/gibbsLDAtmp"
          pathDocument2="/Users/zhaokangpan/IDEA/SogouUserProfile/gibbsLDAtmp2"
        }
        var storedDocuments1=iterativeInputDocuments
        storedDocuments1.persist(StorageLevel.DISK_ONLY)
        storedDocuments1.saveAsObjectFile(pathDocument1)
        var storedDocuments2=updatedDocuments
        storedDocuments2.persist(StorageLevel.DISK_ONLY)
        storedDocuments2.saveAsObjectFile(pathDocument2)
        saveDocTopicDist(iterativeInputDocuments, pathTopicDistOnDoc + "_" + kTopic + "_" + iter)
        saveWordDistTopic(sc, nkv, nk, allWords, vSize, topKwordsForDebug, pathWordDistOnTopic + "_" + kTopic + "_" + iter)

        //restart Spark to solve the memory leak problem
        sc=restartSpark(sc, scMaster, remote)
        //as the restart of Spark, all of RDD are cleared
        //we need to read files in order to rebuild RDD
        iterativeInputDocuments=sc.objectFile(pathDocument1)
        updatedDocuments=sc.objectFile(pathDocument2)

      }
    }
    //Step6,save the result in HDFS (result part 1: topic distribution of doc, result part 2: top words in each topic)
    var resultDocuments = iterativeInputDocuments
    iterativeInputDocuments.unpersist(blocking = false)
    saveDocTopicDist(resultDocuments, pathTopicDistOnDoc)
    resultDocuments.unpersist(blocking = false)
    saveWordDistTopic(sc, nkv, nk, allWords, vSize, topKwordsForDebug, pathWordDistOnTopic)
    //val finalNmk = iterativeInputDocuments.map( t => (t._1, t._2, t._4)).collect
    //saveNkvNkNmk(sc, vSize, nkv, nk, finalNmk)
  }

  def main(args: Array[String]) {

    val fileName = "train_nouns.txt"
    val kTopic = 30
    val alpha = 0.45
    val beta = 0.01
    val maxIter = 100
    val remote = false
    val iterflag = 30
    val rate = 1
    val topKwordsForDebug = 10
    var pathTopicDistOnDoc = ""
    var pathWordDistOnTopic = ""
    if (remote == true) {
      pathTopicDistOnDoc = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/out/topicDistOnDoc"
      pathWordDistOnTopic = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/out/wordDistOnTopic"
    } else {
      pathTopicDistOnDoc = "/Users/zhaokangpan/IDEA/SogouUserProfile/topicDistOnDoc"
      pathWordDistOnTopic = "/Users/zhaokangpan/IDEA/SogouUserProfile/wordDistOnTopic"
    }
    lda(fileName, kTopic, alpha, beta, maxIter, remote, topKwordsForDebug, pathTopicDistOnDoc, pathWordDistOnTopic, iterflag, rate)
  }

}