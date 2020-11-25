package ml.clustering

import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter

object DateFormatterMain  extends  App {

    var format =new SimpleDateFormat("EEE MMM dd kk:mm:ss z yyyy")
    var date ="Wed Jun 29 00:45:01 +0000 2011"

  println(format.parse(date))

  println( LocalDateTime.parse(date,DateTimeFormatter.ofPattern("EEE MMM dd kk:mm:ss +0000 yyyy")).toEpochSecond(ZoneOffset.UTC))
}
