package io.github.kory33.guardedqueries.core.utils.algorithms

import io.github.kory33.guardedqueries.core.utils.algorithms.SimpleUnionFindTree
import io.github.kory33.guardedqueries.testutils.scalacheck.SetGen
import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*

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
  edges <- SetGen.chooseSubset(squareOfDomain.toSet)
} yield UnionFindInput(domainToIdentify.toSet, edges)

object SimpleUnionFindTreeSpec extends Properties("SimpleUnionFindTree") {

  import Prop.forAll

  override def overrideParameters(p: Test.Parameters): Test.Parameters = p.withMinSuccessfulTests(800)

  property("getEquivalenceClasses should output disjoint sets") = forAll(genUnionFindInput) { input =>
    val tree = input.runOnFreshUnionFindTree
    val equivalenceClasses = tree.getEquivalenceClasses.asScala.map(_.asScala.toSet).toSet

    equivalenceClasses.forall { classA =>
      equivalenceClasses.forall { classB =>
        classA == classB || classA.intersect(classB).isEmpty
      }
    }
  }

  property("getEquivalenceClasses should output sets that cover the input") = forAll(genUnionFindInput) { input =>
    val tree = input.runOnFreshUnionFindTree
    val equivalenceClasses = tree.getEquivalenceClasses.asScala.map(_.asScala.toSet).toSet

    equivalenceClasses.flatten == input.collection
  }

  property("getEquivalenceClasses should output equivalence classes that contain all identification edges") = forAll(genUnionFindInput) { input =>
    val tree = input.runOnFreshUnionFindTree
    val equivalenceClasses = tree.getEquivalenceClasses.asScala.map(_.asScala.toSet).toSet

    input.identifications.forall { case (a, b) =>
      equivalenceClasses.exists { equivalenceClass =>
        equivalenceClass.contains(a) && equivalenceClass.contains(b)
      }
    }
  }

  property("getEquivalenceClasses should output equivalence classes in which every pair of elements from the same class are connected by a zig-zag of edges") = forAll(genUnionFindInput) { input =>
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
