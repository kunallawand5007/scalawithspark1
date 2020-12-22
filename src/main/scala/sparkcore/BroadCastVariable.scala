package sparkcore

import com.kj.spark.session.Context

import scala.collection.mutable.ListBuffer

object BroadCastVariable extends App with Context {

  // Simple Demo of how broadcast variable works in spark
  //Broadcast variables are read-only shared variables that are cached and available on all nodes in a cluster in-order to access or use by the tasks
  // Broadcast variables are not sent to executors with sc.broadcast(variable) call instead,
  // they will be sent to executors when they are first used

  case class Person(fname: String, lname: String, state: String)

  var persons: ListBuffer[Person] = new ListBuffer[Person]()

  var p1: Person = new Person("kunal", "lawand", "MH")
  var p2: Person = new Person("Manoj", "Lathiya", "GJ")
  var p3: Person = new Person("Shambu", "kumar", "BR")
  var p4: Person = new Person("Mamta", "Banarji", "WB")
  var p5: Person = new Person("Jagan", "Reddy", "AP")


  persons.append(p1)
  persons.append(p2)
  persons.append(p3)
  persons.append(p4)
  persons.append(p5)

  //persons.foreach(p=>println(p.fname))

  var mapper = Map(("MH", "Maharashtra"), ("GJ", "Gujarat"), ("BR", "Bihar"), ("WB", "WestBengal"), ("AP", "Andrapradesh"))


  val broadcasts = sparkSession.sparkContext.broadcast(mapper)

  //Implementation with RDD
  var personRDD = sparkSession.sparkContext.parallelize(persons)
  //personRDD.foreach(println)

  var updatedRDD = personRDD.map(p => {
    var updatedState = broadcasts.value.get(p.state).get
    (p.fname, p.lname, updatedState.toString)
  })

  var str = updatedRDD.collect().mkString
  println(str)

  // Implementation with DF

  import sparkSession.sqlContext.implicits._

  var personsDF = persons.toDF();
  personsDF.show()
  //Output:
  /* +------+-------+-----+
   | fname|  lname|state|
   +------+-------+-----+
   | kunal| lawand|   MH|
   | Manoj|Lathiya|   GJ|
   |Shambu|  kumar|   BR|
   | Mamta|Banarji|   WB|
   | Jagan|  Reddy|   AP|
   +------+-------+-----+  */

  var updatedDF = personsDF.map(p => {
    var state = broadcasts.value.get(p.getString(2)).get
    (p.getString(0), p.getString(1), state)
  })

  updatedDF.show()
  //Output:

  /* +------+-------+------------+
|    _1|     _2|          _3|
+------+-------+------------+
| kunal| lawand| Maharashtra|
| Manoj|Lathiya|     Gujarat|
|Shambu|  kumar|       Bihar|
| Mamta|Banarji|  WestBengal|
| Jagan|  Reddy|Andrapradesh|
+------+-------+------------+*/


}
