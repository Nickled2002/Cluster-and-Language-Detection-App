import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.immutable._

object LanguageDetectionApp {

    def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("Language Detection App")
	val sc = new SparkContext(conf)
	val bookfiles = sc.textFile("hdfs://hadoopmaster:9000/user/hadoop/books/*.txt")
	val engdict = sc.textFile("hdfs://hadoopmaster:9000/user/hadoop/languages/eng.txt")
	val frdict = sc.textFile("hdfs://hadoopmaster:9000/user/hadoop/languages/fr.txt")
	val gerdict = sc.textFile("hdfs://hadoopmaster:9000/user/hadoop/languages/ger.txt")
	val bookwords = bookfiles.flatMap(x=> x.split(" "))
	val bookwordsPerFiles = bookwords.map(x=>(x,1))
	val reduce =  bookwordsPerFiles.reduceByKey(_+_)
	val engmap = engdict.flatMap(x=> x.split("\n"))
	val frmap = frdict.flatMap(x=> x.split("\n"))
	val germap = gerdict.flatMap(x=> x.split("\n"))
	val reducetupple = reduce.collect()
	//var finallist = ("Eng" -> "0","Fr" -> "0", "Ger" ->"0")	
	var engnumber = 0
	var frnumber = 0
	var gernumber = 0
	for (e <- engmap.collect()){
	var checke = reduce.lookup(e)
	var existse = checke.isEmpty
	if(!existse)
	{
	engnumber = engnumber + checke(0)
	}
	}
	for (f <- frmap.collect()){
	var checkf = reduce.lookup(f)
	var  existsf = checkf.isEmpty
	if(!existsf)
	{
	frnumber = frnumber + checkf(0)
	}
	
	}
	for (g <- germap.collect()){
	var checkg = reduce.lookup(g)
	var  existsg = checkg.isEmpty
	if(!existsg)
	{
	gernumber = gernumber + checkg(0)
	}
	}
	
	
	var toplang = ""
	var topwords = 0
	//bookwordsPerFiles.collect().foreach(println)
	//finallist.foreach(println)
	if ((engnumber>frnumber) && (engnumber>gernumber))
	{
	toplang = "English"
	topwords = engnumber
	}
	if ((frnumber>engnumber) && (frnumber>gernumber))
	{
	toplang = "French"
	topwords = frnumber
	}
	if ((gernumber>frnumber) && (gernumber>engnumber))
	{
	toplang = "German"
	topwords = gernumber
	}
	
	printf("There are %d", engnumber)
	println(" english words.")
	printf("There are %d", frnumber)
	println(" french words.")
	printf("There are %d", gernumber)
	println(" german words.")
	print("Therefore the most used language in the directory is ")
	print(toplang)
	print(" with a total of ")
	print(topwords)
	println(" words used.")
	//engmap.collect().foreach(println)
	//frmap.collect().foreach(println)
	//reduce.collect().foreach(println)

     }
}
