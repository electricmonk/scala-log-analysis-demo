package com.shaiyallin.logs

import java.time.{ZoneOffset, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.io.{Codec, Source}
import com.netaporter.uri.Uri
import com.netaporter.uri.parsing.UriParser
import com.netaporter.uri.config.UriConfig

/**
 * @author shaiyallin
 * @since 7/18/14
 */

case class Request(ip: String, date: LocalDateTime, method: String, uri: Uri, status: Int, responseSize: Int, userAgent: String)

object Parse {
  def unapply(uri: String): Option[Uri] = new UriParser(uri, UriConfig.default)._uri.run().toOption
}

object Request {
  val NCSALogLine = "^([0-9.]*).*?\\[(\\w.*)?\\]\\s\\\"(\\w*)\\s([\\S]*).*\\\"\\s(\\d*)?\\s(\\d*)?\\s.*\\s\\\"(.*)?\\\".*$".r
  val dateFormat = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z").withZone(ZoneOffset.UTC)

  def unapply(line: String): Option[Request] = line match {
    case NCSALogLine(ip, date, method, Parse(uri), status, responseSize, userAgent) =>
      Some(Request(ip, parseDate(date), method, uri, status.toInt, responseSize.toInt, userAgent))
    case _ => None
  }

  val parse: PartialFunction[String, Request] = { case Request(request) => request }

  def parseDate(date: String) = LocalDateTime.parse(date, dateFormat)


}

object Logs extends App {
  import scala.io._
  import java.time._

  type Requests = Seq[Request]
  implicit val codec = Codec.ISO8859
  val log = Source.fromFile("./request.100th.log")
  val allRequests = log.getLines().collect(Request.parse).toList

  val subnetOf = { ip: String => if (ip.contains(".")) ip.substring(0, ip.lastIndexOf('.')) else ip }
  val statusIs = { f: (Int => Boolean) => requests: Requests => requests.filter(r => f(r.status))}
  val popularPaths = { requests: Requests => requests.groupBy(r => r.uri.path).toSeq.sortBy { case (path, rs) => -rs.size} }

  val errors = statusIs(_ >= 400)
  
  val errorRateFor = { requests: Requests =>
    val erroneous = errors(requests)
    f"Error rate: ${erroneous.size * 100.0 / requests.size}%2.2f%%"
  }

  val totalCountFor = { requests: Requests => f"Total requests: ${requests.size}%,d" }

  val sizes = { requests: Requests => requests.map(_.responseSize)}
  val totalSizeFor = { requests: Requests => f"Total response size: ${sizes(requests).sum}%,d bytes" }

  val summary = { rs: Requests => Seq(totalCountFor(rs), totalSizeFor(rs), errorRateFor(rs)) }

  println(summary(allRequests).mkString("\n"))

  val (subnet, requestsFromMostCommonSubnet) = allRequests.groupBy(r => subnetOf(r.ip)).maxBy {case (_, rs) => rs.size}
  println(f"Most common subnet:\n\t${summary(requestsFromMostCommonSubnet).mkString("\n\t")}")

  println(f"10 Most popular paths:")
  popularPaths(allRequests).take(10) foreach { case (path, rs) => println(f"$path\n\t${summary(rs).mkString("\n\t")}")}


  val mostErroneous = popularPaths(errors(allRequests)).head
  val errorsByType = mostErroneous._2.groupBy(_.status).map { case (status, rs) => s"${rs.size} requests resulting in status $status"}
  println(f"Most erroneous path: ${mostErroneous._1}\n\t${errorsByType.mkString("\n\t")}")

  allRequests.groupBy(request => request.date.withSecond(0).withMinute(0)).toSeq.sortBy { case (date, rs) => date.toEpochSecond(ZoneOffset.UTC)}.map {case (date, rs) =>
    println(s"$date: \n\t${summary(rs).mkString("\n\t")}")
  }
}
