//Case: Movement Track of Mobile Users
/** 
  *  Task: Read mobile stations and user information files from HDFS, calculated the duration of stay in different stations per user in Spark with scala and matched with geographic information
  *  mobile_station.log: phone number，enter time of station，station id，event type (enter=1 out=0)
  *  			  eg: 13031173199 20180727150603 ASCIM66777 1
  *  loc_info.txt: station id, latitude，longtitude
  *  		   eg: ASCIM66777 140.36 35.56
  * 
  */

object  MobileStation {

	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("ObjectCount").setMaster("local[2]")
		val sc: SparkContext = new SparkContext(conf)
		//read Log file
		val file: RDD[String] = sc.textFile("C://Users/Angela/Documents/mobile_station.log")
		//phoneNumber location time
		val phoAndStaAndTime: RDD[(String, String), Long, Long)] = file.map(line => {
			val fields = line.map(_.split("\t"))
			val phone = line(0)
			val time = line(1).toLong
			val id = line(2)
			val event = line(3)
			//correct time expression
			val cTime = if (event == 1) time else -time
			((phone, id), time, cTime)
			}).filter(
				var flag = false
				if(time > 20180727000000 && time < 20180728000000){
					flag=true
				}
				flag
			)
		//timePeriod during certain station
		val user: RDD[(String, String), Long)] = file.map(line => {
			val phone = line._1._1
			val cTime = line._3
			val id = line._1._1
			((phone, id), cTime)
			}).reduceByKey(_+_)
		//Variable consists of station phoneNumber timePeriod
		val staAndPhoAndTime: RDD[(String, (String, Long))] = user.map(line => {
				val phone = line._1._1
				val id = line._1._2
				val cTime = line._2
				(id, (phone, cTime))
			})
		//location latitude longtitude
		val lFile = sc.readFile("C://Users/Angela/Documents/loc_info.txt")
		val locAndLaAndLon: RDD[(String, (String, String))] = lFile.map(line => {
			val columns = line.map(_.split("\t"))
			val id = columns(0)
			val lat = columns(1)
			val lon = columns(2)
			(id,(lat,lon))
			})
		//connection
		val fullInfo: RDD[(String, (String, Long), (String, String))] = 
			staAndPhoAndTime.join(locAndLaAndLon)
		//group with phoneNumber, sort by time and take top 3
		val res: RDD[(String, List((String, Long), (String, String)))] = fullInfo.map(line => {
			val phone = line._2._1._1
			val time = line._2._1._2
			val loc = line._2._2
			(phone, time, loc)
			}).groupBy(_._1).mapValues(_.toList.sortBy(_._2).reverse.take(3))
		sc.stop()
	}
}
