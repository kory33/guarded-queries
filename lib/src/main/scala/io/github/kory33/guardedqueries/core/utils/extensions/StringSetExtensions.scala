package io.github.kory33.guardedqueries.core.utils.extensions

import java.util
import scala.annotation.tailrec

object StringSetExtensions {
  def isPrefixOfSome(strings: util.Collection[String], string: String): Boolean = {
    import scala.jdk.CollectionConverters._
    strings.asScala.exists(_.startsWith(string))
  }

  /**
   * Pick a string starting with {@code startingWith}, that is not a prefix of any string from
   * {@code strings}.
   */
  def freshPrefix(strings: util.Collection[String], startingWith: String): String = {
    @tailrec def searchForFreshPrefix(count: Long): String = {
      val candidate = startingWith + count.toHexString
      if (!isPrefixOfSome(strings, candidate)) candidate
      else searchForFreshPrefix(count + 1)
    }

    searchForFreshPrefix(0)
  }
}
