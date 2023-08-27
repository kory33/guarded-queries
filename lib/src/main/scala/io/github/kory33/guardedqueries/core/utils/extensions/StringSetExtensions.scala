package io.github.kory33.guardedqueries.core.utils.extensions

import java.util

object StringSetExtensions {
  def isPrefixOfSome(strings: util.Collection[String], string: String): Boolean = {
    import scala.collection.JavaConversions._
    for (s <- strings) { if (s.startsWith(string)) return true }
    false
  }

  /**
   * Pick a string starting with {@code startingWith}, that is not a prefix of any string from
   * {@code strings}.
   */
  def freshPrefix(strings: util.Collection[String], startingWith: String): String = {
    var count = 0
    while (true) {
      val candidate = startingWith + Long.toHexString(count)
      if (!isPrefixOfSome(strings, candidate)) return candidate
      count += 1
    }
  }
}
