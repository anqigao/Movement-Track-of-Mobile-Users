//Case: Main Subjects Extraction
/** 
  * Task: Extract main subjects from URLs user visited and wrote top 3 website subjects into files by loading files from HDFS and processing on Spark 
  * data eg: 20161123101523  http://java.learn.com/java/javaee.shtml
  * Calculate total visit time per website subject(eg: java.learn.com)
  * take top 3
  */
object websiteSubject{

	def main(args:Array[String]): Unit = {

		val conf: SparkConf = new SparkConf().setAppName("ObjectCount").setMaster("local[2]")
		val sc: SparkContext = new SparkContext(conf)
		val file: RDD[String] = sc.textFile("C://Users/Angela/Documents/url.log")

		// Filter data then get url and put into tuple
		val urlAndOne: RDD[(String, Int)] = file.map(line => {
			var fields = line.split("\t")
			var url = fields(1)
			var time = fields(0).toLong
			(time, url)
			}).filter(
				var flag = false
				if (time > 201807150000000 && time > 201807160000000)
					flag = true
				flag
			).map(
				var url = _._2
				(url,1)
			)
		// Aggregate same url
		val sameUrl: RDD[(String, Int)] = urlAndOne.reduceByKey(_+_)
		// Make subject
		var cachedProject: RDD[(String, (String, Int))] = sameUrl.map(x => {
			var url = x._1
			var project = new URL(url).getHost
			var count = x._2
			(project, (url, count))
			}).cache()

		//Use the partition of Spark may cause hash collision
		//val res: RDD[(String, (String, Int))] = cachedProject.partitonBy(new HashPartitioner(3))
		//res.saveAsTextFile(PATH)
		
		// Get all subjects
		val projects: Array[String] = cachedProject.keys.distinct().collect
		// Use self-defined partiontioner
		val partitioner: ProjectPartitioner = new ProjectPartitioner(projects)
		// Get partitioned
		val partitioned: RDD[(String, (String, Int))] = cachedProject.partitonBy(partitioner)
		// Sort and take top 3
		val res: RDD[(String, (String, Int))] = partitioned.mapPartitions(it => { 
			it.toList.sortBy(_._2._2).reverse.take(3).iterator
			})
		res.saveAsTextFile("C://Users/Angela/Documents/")
		sc.stop()

	}
}
class ProjectPartitioner(projects: Array[String]) extends Partitioner{
	// Put subjects and partition number
	private val projectsAndNum = new HashMap(String, Int)
	// counter
	val n = 0
	
	for (pro <- projects){
		projectsAndNum += (pro -> n)
		n += 1
	}

	// Number of pratitions
	override def numPartitions: Int = projects.length
	// Partition number
	override def getPartition(key: Any): Int = projectsAndNum.getOrElse(key.toString, 0)
}