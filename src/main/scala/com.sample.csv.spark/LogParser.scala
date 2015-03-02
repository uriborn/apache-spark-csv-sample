package com.sample.csv.spark

import java.text.SimpleDateFormat
import scala.util.parsing.combinator._

case class Log(time: String = "", url: String = "")

object LogParser extends RegexParsers {

  def parse(log: String): ParseResult[Log] = parseAll(line, log)

  private def line: Parser[Log] =
    time ~ logLevel ~ method ~ url ~ routes ~ controller ~ returned ~ status ~ in ~ procTime ~
    where ~ requestId ~ and ~ remoteAddress ~ and ~ userAgent ^^ {
      case time ~ logLevel ~ method ~ url ~ routes ~ controller ~ returned ~ status ~ in ~ procTime ~
           where ~ requestId ~ and1 ~ remoteAddress ~ and2 ~ userAgent => Log(time, url)
    }

  private def time: Parser[String] = "[" ~> """\S+ [^ \]]+""".r <~ "]" ^^ { dayFloor }
  private def logLevel: Parser[String] = "[" ~> """\S+""".r <~ "]"
  private def method: Parser[String] = """[A-Z]+""".r
  private def url: Parser[String] = """\S+""".r
  private def routes: Parser[String] = "routes".r
  private def controller: Parser[String] = """\S+""".r
  private def returned: Parser[String] = "returned".r
  private def status: Parser[Int] = """\S+""".r ^^ { status => status.split("=")(1).toInt }
  private def in: Parser[String] = "in".r
  private def procTime: Parser[String] = """\S+""".r
  private def where: Parser[String] = "where".r
  private def requestId: Parser[Long] = "[" ~> """\S+=\d+""".r <~ "]" ^^ { requestId => requestId.split("=")(1).toLong }
  private def and: Parser[String] = "and".r
  private def remoteAddress: Parser[String] = """\S+""".r ^^ { remoteAddress => remoteAddress.split("=")(1) }
  private def userAgent: Parser[String] = """[^"]+""".r

  private def dayFloor(timestamp: String): String = {
    val dateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").parse(timestamp)

    new SimpleDateFormat("yyyy-MM-dd").format(dateTime)
  }

}

