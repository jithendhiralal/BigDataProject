import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object pRank {
	def exec() {
	val sconf = new SparkConf()
	val paramsString = sconf.get("spark.driver.extraJavaOptions")
	val paramsSlice = paramsString.slice(2,paramsString.length)
	val paramsArray = paramsSlice.split(",")
	val graph = GraphLoader.edgeListFile(sc, paramsArray(0))
	// Run PageRank
	val ranks = graph.pageRank(0.0001).vertices

	// Join the ranks with the usernames
	val pages = sc.textFile(paramsArray(1)).map { line =>
		val fields = line.split(",")
		(fields(0).toLong, fields(1))
	}
	val ranksByUrl = pages.join(ranks).map {
	case (id, (url, rank)) => (rank, url)
	}
	val sortRanksByUrl = ranksByUrl.sortByKey(false).map(v => v._2 + " " + v._1)
	sortRanksByUrl.saveAsTextFile(paramsArray(2))
	System.exit(0)
	}
}
pRank.exec()