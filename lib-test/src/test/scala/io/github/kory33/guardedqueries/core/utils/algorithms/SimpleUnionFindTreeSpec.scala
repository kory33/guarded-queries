package io.github.kory33.guardedqueries.core.utils.algorithms

import io.github.kory33.guardedqueries.core.utils.algorithms.SimpleUnionFindTree
import io.github.kory33.guardedqueries.testutils.scalacheck.GenSet
import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

/**
 * An input for union-find algorithm;
 */
private[this] case class UnionFindInput(collection: Set[Int], identifications: Set[(Int, Int)]) {
  require(identifications.forall(t => collection.contains(t._1) && collection.contains(t._2)))

  def runOnFreshUnionFindTree: SimpleUnionFindTree[Int] = {
    val tree = new SimpleUnionFindTree(collection.asJava)
    identifications.foreach { case (a, b) => tree.unionTwo(a, b) }
    tree
  }
}

// generate union find input of size at most 6
private[this] val genUnionFindInput = for {
  upperBound <- Gen.chooseNum(0, 6)
  domainToIdentify = 1 to upperBound
  squareOfDomain = domainToIdentify.flatMap(a => domainToIdentify.map(b => (a, b)))
  edges <- GenSet.chooseSubset(squareOfDomain.toSet)
} yield UnionFindInput(domainToIdentify.toSet, edges)

class SimpleUnionFindTreeSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {
  ".getEquivalenceClasses" should "output disjoint sets" in {
    forAll(genUnionFindInput, minSuccessful(1000)) { input =>
      val tree = input.runOnFreshUnionFindTree
      val equivalenceClasses = tree.getEquivalenceClasses.asScala.map(_.asScala.toSet).toSet

      equivalenceClasses.forall { classA =>
        equivalenceClasses.forall { classB =>
          classA == classB || classA.intersect(classB).isEmpty
        }
      }
    }
  }

  ".getEquivalenceClasses" should "output sets that cover the input" in {
    forAll(genUnionFindInput, minSuccessful(1000)) { input =>
      val tree = input.runOnFreshUnionFindTree
      val equivalenceClasses = tree.getEquivalenceClasses.asScala.map(_.asScala.toSet).toSet

      equivalenceClasses.flatten == input.collection
    }
  }

  ".getEquivalenceClasses" should "output equivalence classes that contain all identification edges" in {
    forAll(genUnionFindInput, minSuccessful(1000)) { input =>
      val tree = input.runOnFreshUnionFindTree
      val equivalenceClasses = tree.getEquivalenceClasses.asScala.map(_.asScala.toSet).toSet

      input.identifications.forall { case (a, b) =>
        equivalenceClasses.exists { equivalenceClass =>
          equivalenceClass.contains(a) && equivalenceClass.contains(b)
        }
      }
    }
  }

  ".getEquivalenceClasses" should "output equivalence classes in which every pair of elements from the same class are connected by a zig-zag of edges" in {
    forAll(genUnionFindInput, minSuccessful(1000)) { input =>
      val bidirectionalIdentification = input.identifications.flatMap { case (a, b) => Set((a, b), (b, a)) }

      def existsZigZagPathConnecting(a: Int, b: Int): Boolean = {
        val visited = collection.mutable.Set[Int](a)
        var seenNewElement = true

        // perform simple BFS
        while (seenNewElement) {
          seenNewElement = false
          visited.toSet.foreach(node =>
            val adjacentElements = bidirectionalIdentification.filter(_._1 == node).map(_._2)
            val unvisitedAdjacentElements = adjacentElements.diff(visited)
            if (unvisitedAdjacentElements.nonEmpty) {
              seenNewElement = true
              visited ++= unvisitedAdjacentElements
            }
          )
        }

        visited.contains(b)
      }

      val tree = input.runOnFreshUnionFindTree
      val equivalenceClasses = tree.getEquivalenceClasses.asScala.map(_.asScala.toSet).toSet

      equivalenceClasses.forall { equivalenceClass =>
        equivalenceClass.forall { a =>
          equivalenceClass.forall { b =>
            existsZigZagPathConnecting(a, b)
          }
        }
      }
    }
  }
}
