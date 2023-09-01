package io.github.kory33.guardedqueries.core.utils.algorithms

import scala.annotation.tailrec
import scala.collection.mutable

final class SimpleUnionFindTree[V](values: Set[V]) {
  // invariants:
  //  - referenceTowardsRepresentative.keySet() intersection representatives is empty
  //  - referenceTowardsRepresentative.keySet() union representatives is constant after every operation
  private val referenceTowardsRepresentative: mutable.HashMap[V, V] = mutable.HashMap.empty
  private val representatives: mutable.HashSet[V] = mutable.HashSet(values.toSeq: _*)

  private def representativeOfClassOf(value: V): V = {
    @tailrec def ascendUFTree(current: V): V =
      if representatives.contains(current) then
        current
      else
        referenceTowardsRepresentative.get(current) match
          case Some(parent) => ascendUFTree(parent)
          case None =>
            throw IllegalArgumentException(s"Unrecognized by the UF tree: ${value.toString}")

    ascendUFTree(value)
  }

  def unionTwo(v1: V, v2: V): Unit = {
    val v1Representative = representativeOfClassOf(v1)
    val v2Representative = representativeOfClassOf(v2)
    if (v1Representative != v2Representative) {
      // let v2Representative point to v1Representative
      referenceTowardsRepresentative.put(v2Representative, v1Representative)
      representatives.remove(v2Representative)
    }
  }

  def unionAll(values: List[V]): Unit =
    values match
      case head :: tail => tail.foreach(unionTwo(head, _))
      case Nil          => ()

  def unionAll(values: Iterable[V]): Unit = unionAll(values.toList)

  def getEquivalenceClasses: Set[Set[V]] = {
    val equivClasses = mutable.HashMap.from {
      this.representatives.map { representatives =>
        (representatives, mutable.HashSet[V](representatives))
      }
    }

    for (nonRepresentative <- this.referenceTowardsRepresentative.keySet) {
      equivClasses(representativeOfClassOf(nonRepresentative)).add(nonRepresentative)
    }

    equivClasses.values.map(_.toSet).toSet
  }
}
