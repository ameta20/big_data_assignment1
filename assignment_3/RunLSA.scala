// src/main/scala/RunLSA.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession // For SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors => MLLibVectors} // Renamed to avoid clash
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import scala.io.Source
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.collection.JavaConverters._ // For JavaConversions in newer Scala

// Stanford CoreNLP
import java.util.Properties
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._

// Breeze for linear algebra in queries
import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import org.apache.spark.mllib.linalg.{Matrices, Vector => MLLibVector} // MLLibVector for clarity


object RunLSA {

  // --- Helper Functions from the script (can be private or within object) ---
  private def parseHeader(line: String): Array[String] = {
    try {
      var s = line.substring(line.indexOf("id=\"") + 4)
      val id = s.substring(0, s.indexOf("\""))
      s = s.substring(s.indexOf("url=\"") + 5)
      val url = s.substring(0, s.indexOf("\""))
      s = s.substring(s.indexOf("title=\"") + 7)
      val title = s.substring(0, s.indexOf("\""))
      Array(id, url, title)
    } catch {
      case e: Exception => Array("", "", "")
    }
  }

  private def parse(lines: Array[String]): Array[(String, String)] = {
    val docs = ArrayBuffer.empty[(String, String)]
    var title = ""
    var content = ""
    for (line <- lines) {
      try {
        if (line.startsWith("<doc ")) {
          title = parseHeader(line)(2)
          content = ""
        } else if (line.startsWith("</doc>")) {
          if (title.nonEmpty && content.nonEmpty) {
            docs += ((title, content))
          }
        } else {
          content += line + "\n"
        }
      } catch {
        case e: Exception => content = "" // Reset content on error
      }
    }
    docs.toArray
  }

  private def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
  }

  // --- Stanford NLP Pipeline Functions ---
  private def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }

  // Modified to take broadcasted stopwords
  private def plainTextToLemmas(text: String, pipeline: StanfordCoreNLP, stopWords: Set[String]): Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (
      sentence <- sentences.asScala; // Use asScala for Java collections
      token <- sentence.get(classOf[TokensAnnotation]).asScala
    ) {
      val lemma = token.get(classOf[LemmaAnnotation]).toLowerCase
      if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
        lemmas += lemma
      }
    }
    lemmas.toSeq // Return immutable Seq
  }

  // --- Regex Tokenizer Functions (New) ---
  val WORD_PATTERN = "^[a-z-]{6,24}$".r
  val NUMBER_PATTERN = "^-?[0-9.]{4,16}$".r
  val NUMBER_STRUCTURE_PATTERN = "^-?[0-9.]+$".r // To ensure only digits and dots after optional dash
  val TOKEN_FINDER_PATTERN = "[a-z0-9.-]+".r

  private def regexTokenize(text: String, stopWords: Set[String]): Seq[String] = {
    val lowerText = text.toLowerCase
    TOKEN_FINDER_PATTERN.findAllIn(lowerText).flatMap { token =>
      if (WORD_PATTERN.pattern.matcher(token).matches && !stopWords.contains(token)) {
        Some(token)
      // } else if (NUMBER_PATTERN.pattern.matcher(token).matches && NUMBER_STRUCTURE_PATTERN.pattern.matcher(token).matches() ) {
      //  Some(token) // Decide if numbers should be included in LSA term vocabulary
      } else {
        None
      }
    }.toSeq
  }


  // --- LSA Query Functions (modified to use Maps directly and be more robust) ---
  private def topTermsInTopConcepts(
    svd: SingularValueDecomposition[RowMatrix, Matrix],
    numConcepts: Int,
    numTermsToDisplay: Int, // Renamed for clarity
    termIds: Map[Int, String] // Pass termIds map
  ): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1) // Sort by weight descending
      topTerms += sorted.take(numTermsToDisplay).map {
        case (score, id) => (termIds.getOrElse(id, "UNKNOWN_TERM"), score)
      }
    }
    topTerms.toSeq
  }

  private def topDocsInTopConcepts(
    svd: SingularValueDecomposition[RowMatrix, Matrix],
    numConcepts: Int,
    numDocsToDisplay: Int, // Renamed for clarity
    docIdsMap: Map[Long, String] // Pass docIdsMap
  ): Seq[Seq[(String, Double)]] = {
    val u = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    if (u == null) { // Check if U was computed
        println("WARN: U matrix (document-concept vectors) was not computed. Cannot get top docs.")
        return Seq.empty
    }
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId() // This gives RDD[(Double, Long)]
      // Collect and sort top N on the driver. For very large U, this could be an issue.
      val sortedDocs = docWeights.top(numDocsToDisplay)(Ordering.by(_._1)) // Sort by score descending
      topDocs += sortedDocs.map {
        case (score, id) => (docIdsMap.getOrElse(id, "UNKNOWN_DOC_ID_" + id), score)
      }.toSeq
    }
    topDocs.toSeq
  }

   // --- Keyword Query Functions ---
  private def termsToQueryVector(
    terms: Seq[String], // Use Seq
    idTerms: Map[String, Int], // Pass Map
    idfs: Map[String, Double] // Pass Map
  ): BSparseVector[Double] = {
    val S = terms.size
    val G = idTerms.size
    val termIndices = new ArrayBuffer[Int]()
    val termValues = new ArrayBuffer[Double]()

    terms.foreach{ term =>
        if (idTerms.contains(term) && idfs.contains(term)){
            termIndices += idTerms(term)
            termValues += idfs(term)
        } else {
            println(s"Warning: Term '$term' not in vocabulary or IDF map, skipping for query.")
        }
    }
    new BSparseVector[Double](termIndices.toArray, termValues.toArray, G)
  }

  private def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map { vec =>
      val vecArr = vec.toArray
      val newArr = vecArr.indices.map(i => vecArr(i) * sArr(i)).toArray // Ensure it's an Array[Double]
      MLLibVectors.dense(newArr)
    })
  }

  private def topDocsForTermQuery(
    US: RowMatrix, // U * S
    V: Matrix,     // V matrix from SVD
    query: BSparseVector[Double],
    docIdsMap: Map[Long,String], // Pass docIdsMap
    numDocsToDisplay: Int = 10 // Default to 10
  ): Seq[(String, Double)] = { // Return Seq of (docTitle, score)
    val breezeV = new BDenseMatrix[Double](V.numRows, V.numCols, V.toArray)
    val termRowArr = (breezeV.t * query).toArray // Concept weights for the query
    val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)

    val docScores = US.multiply(termRowVec) // Multiply U*S by (V.t * query)
    
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId() // RDD[(Double, Long)] (score, docID)
    allDocWeights.top(numDocsToDisplay)(Ordering.by(_._1)).map{ // Get top N by score
        case (score, id) => (docIdsMap.getOrElse(id, "UNKNOWN_DOC_ID_" + id), score)
    }.toSeq
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    if (args.length < 6) {
      System.err.println(
        "Usage: RunLSA <wikiDataPath> <stopwordsPath> <tokenizerType (nlp|regex)> " +
        "<numLatentConceptsK> <numTermsVocabulary> <sampleFraction (0.0 to 1.0)>"
      )
      System.exit(1)
    }

    val wikiDataPath = args(0)         // e.g., "hdfs:///path/to/Wikipedia-En-41784-Articles/*/*" or "local_path/*/*"
    val stopwordsPath = args(1)      // e.g., "hdfs:///path/to/stopwords.txt" or "local_path/stopwords.txt"
    val tokenizerType = args(2).toLowerCase
    val k = args(3).toInt              // Number of latent concepts
    val numTermsVocabulary = args(4).toInt // Max terms for TF-IDF vocabulary
    val sampleFraction = args(5).toDouble

    val spark = SparkSession.builder
      .appName(s"RunLSA-$tokenizerType-k$k-terms$numTermsVocabulary-sample$sampleFraction")
      .getOrCreate()
    val sc = spark.sparkContext

    println(s"Starting LSA with: tokenizer=$tokenizerType, k=$k, numTermsVocab=$numTermsVocabulary, sample=$sampleFraction")
    println(s"Wiki data path: $wikiDataPath")
    println(s"Stopwords path: $stopwordsPath")


    // --- 1. Parse Wikipedia Articles ---
    // Note: For local files, prefix path with "file://". For HDFS, use "hdfs://" or just "/".
    val textFiles = sc.wholeTextFiles(wikiDataPath).sample(withReplacement = false, fraction = sampleFraction, seed = 42L)
    val numFiles = textFiles.count()
    println(s"Sampled $numFiles text files.")
    if (numFiles == 0) {
        println("No files found in input path or after sampling. Exiting.")
        spark.stop()
        return
    }

    val plainTextRDD: RDD[(String, String)] = textFiles.flatMap { case (_, text) => parse(text.split("\n")) }
    plainTextRDD.cache() // Cache for multiple uses
    val numDocs = plainTextRDD.count()
    println(s"Parsed $numDocs documents.")
    if (numDocs == 0) {
        println("No documents parsed. Exiting.")
        spark.stop()
        return
    }


    // --- 2. Tokenization & Lemmatization / Regex Tokenization ---
    val bStopWords = sc.broadcast(Source.fromFile(stopwordsPath).getLines().toSet) // Assuming stopwords file is accessible by driver

    val tokenizedRDD: RDD[(String, Seq[String])] = tokenizerType match {
      case "nlp" =>
        println("Using NLP (Stanford CoreNLP) tokenizer and lemmatizer...")
        plainTextRDD.mapPartitions { iter =>
          val pipeline = createNLPPipeline() // Create one pipeline per partition
          iter.map { case (title, contents) =>
            (title, plainTextToLemmas(contents, pipeline, bStopWords.value))
          }
        }
      case "regex" =>
        println("Using Regex tokenizer...")
        plainTextRDD.map { case (title, contents) =>
          (title, regexTokenize(contents, bStopWords.value))
        }
      case _ =>
        println(s"Error: Unknown tokenizer type '$tokenizerType'. Use 'nlp' or 'regex'.")
        spark.stop()
        return
    }
    tokenizedRDD.cache()

    // --- 3. Compute Term Frequencies and Document Frequencies ---
    val docTermFreqsRDD: RDD[(String, HashMap[String, Int])] = tokenizedRDD.map {
      case (title, terms) =>
        val termFreqs = terms.foldLeft(new HashMap[String, Int]()) {
          (map, term) => map += term -> (map.getOrElse(term, 0) + 1); map
        }
        (title, termFreqs)
    }
    docTermFreqsRDD.cache()
    println(s"Computed document term frequencies for ${docTermFreqsRDD.count()} documents.")

    // Create a global map from document title to a unique Long ID
    // CAUTION: collectAsMap can be an issue for very large number of documents.
    // For this dataset size, it should be okay.
    val docIdsMap: Map[Long, String] = docTermFreqsRDD.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()
    println(s"Generated ${docIdsMap.size} unique document IDs.")


    // Calculate document frequency for each term
    val docFreqsRDD = docTermFreqsRDD.flatMap(_._2.keySet).map((_, 1)).reduceByKey(_ + _)

    // Select top N terms for the vocabulary based on document frequency
    // Use numTermsVocabulary passed as argument
    val ordering = Ordering.by[(String, Int), Int](_._2) // Order by count (value)
    val topDocFreqTerms = docFreqsRDD.top(numTermsVocabulary)(ordering.reverse) // Get least frequent if not reversed, so reverse for most freq
    println(s"Selected top ${topDocFreqTerms.length} terms for vocabulary.")


    // --- 4. Compute TF-IDF Scores ---
    val bNumDocs = sc.broadcast(numDocs.toDouble) // Broadcast total number of documents

    val idfs: Map[String, Double] = topDocFreqTerms.map {
      case (term, count) => (term, math.log(bNumDocs.value / count.toDouble))
    }.toMap
    
    val idTerms: Map[String, Int] = idfs.keys.zipWithIndex.toMap // Term to Integer ID
    val termIds: Map[Int, String] = idTerms.map(_.swap)         // Integer ID to Term

    val bIdfs = sc.broadcast(idfs)
    val bIdTerms = sc.broadcast(idTerms) // Broadcast term to int ID map

    // Create document vectors (TF-IDF)
    val docVectorsRDD: RDD[MLLibVector] = docTermFreqsRDD.map(_._2).map { termFreqs =>
      val docTotalTerms = termFreqs.values.sum.toDouble
      if (docTotalTerms == 0) MLLibVectors.sparse(bIdTerms.value.size, Seq()) // Handle empty docs
      else {
          val termScores = termFreqs.filter { case (term, _) => bIdTerms.value.contains(term) }
            .map { case (term, freq) =>
                (bIdTerms.value(term), (bIdfs.value(term) * freq.toDouble) / docTotalTerms)
            }.toSeq // Seq[(Int, Double)]
          MLLibVectors.sparse(bIdTerms.value.size, termScores)
      }
    }
    docVectorsRDD.cache()
    println(s"Created ${docVectorsRDD.count()} TF-IDF document vectors.")

    // --- 5. Compute SVD ---
    if (docVectorsRDD.isEmpty()) {
        println("No document vectors to process for SVD. Exiting.")
        spark.stop()
        return
    }
    val mat = new RowMatrix(docVectorsRDD)
    println(s"Computing SVD with k=$k...")
    val svd = mat.computeSVD(k, computeU = true) // computeU=true is needed for top docs
    println("SVD computation finished.")


    // --- 6. Query the Latent Semantic Index ---
    val conceptsToDisplay = 25
    val termsPerConcept = 25
    val docsPerConcept = 25

    println(s"\n--- Top $termsPerConcept Terms in Top $conceptsToDisplay Concepts ---")
    val topConceptTerms = topTermsInTopConcepts(svd, conceptsToDisplay, termsPerConcept, termIds)

    println(s"\n--- Top $docsPerConcept Docs in Top $conceptsToDisplay Concepts ---")
    val topConceptDocs = topDocsInTopConcepts(svd, conceptsToDisplay, docsPerConcept, docIdsMap)

    topConceptTerms.zipWithIndex.zip(topConceptDocs).foreach {
      case (((terms, conceptIdx), docs)) =>
        println(s"\nConcept ${conceptIdx + 1}:")
        println(s"  Top Terms: " + terms.map { case (term, weight) => f"$term ($weight%.3f)" }.mkString(", "))
        println(s"  Top Docs:  " + docs.map { case (doc, weight) => f"$doc ($weight%.3f)" }.mkString(", "))
    }
    println("\nLSA analysis complete for top concepts/terms/docs.")

    // --- Optional: Keyword Queries Example (as in original script) ---
    println("\n--- Example Keyword Query ---")
    val US = multiplyByDiagonalRowMatrix(svd.U, svd.s) // U * Sigma
    val queryTerms = Seq("germany", "berlin") // Example query terms
    println(s"Querying for terms: ${queryTerms.mkString(", ")}")

    val queryVec = termsToQueryVector(queryTerms, bIdTerms.value, bIdfs.value)
    if (queryVec.activeSize > 0) { // Check if any query terms were found in vocabulary
        val relevantDocs = topDocsForTermQuery(US, svd.V, queryVec, docIdsMap, numDocsToDisplay = 10)
        println("Top 10 documents for query:")
        relevantDocs.foreach { case (docTitle, score) =>
            println(f"  $docTitle (Score: $score%.4f)")
        }
    } else {
        println("None of the query terms were found in the vocabulary. Cannot perform query.")
    }


    spark.stop()
  }
}
