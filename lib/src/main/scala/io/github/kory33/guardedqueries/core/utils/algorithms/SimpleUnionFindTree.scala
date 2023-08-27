package io.github.kory33.guardedqueries.core.utils.algorithms

import com.google.common.collect.ImmutableSet
import java.util

final class SimpleUnionFindTree[V](values: util.Collection[_ <: V]) {
  // invariants:
  //  - referenceTowardsRepresentative.keySet() intersection representatives is empty
  //  - referenceTowardsRepresentative.keySet() union representatives is constant after every operation
  private var referenceTowardsRepresentative: util.HashMap[V, V] = new util.HashSet[V](values)
  private var representatives: util.HashSet[V] = new util.HashMap[V, V]

  def representativeOfClassOf(value: V): V = {
    var current = value
    while (true) if (representatives.contains(current)) return current
    else if (referenceTowardsRepresentative.containsKey(current))
      current = referenceTowardsRepresentative.get(current)
    else throw new IllegalArgumentException("Unrecognized by the UF tree: " + value.toString)
  }

  def unionTwo(v1: V, v2: V): Unit = {
    val v1Representative = representativeOfClassOf(v1)
    val v2Representative = representativeOfClassOf(v2)
    if (v1Representative ne v2Representative) {
      // let v2Representative point to v1Representative
      referenceTowardsRepresentative.put(v2Representative, v1Representative)
      representatives.remove(v2Representative)
    }
  }

  def unionAll(values: util.Collection[V]): Unit = {
    val iterator = values.iterator
    if (!iterator.hasNext) return
    val first = iterator.next
    iterator.forEachRemaining((value: V) => unionTwo(first, value))
  }

  def getEquivalenceClasses: ImmutableSet[ImmutableSet[V]] = {
    val equivClasses = new util.HashMap[V, util.HashSet[V]]
    import scala.collection.JavaConversions._
    for (representative <- this.representatives) {
      val freshClass = new util.HashSet[V]
      freshClass.add(representative)
      equivClasses.put(representative, freshClass)
    }
    import scala.collection.JavaConversions._
    for (nonRepresentative <- this.referenceTowardsRepresentative.keySet) {
      equivClasses.get(this.representativeOfClassOf(nonRepresentative)).add(nonRepresentative)
    }
    ImmutableSet.copyOf(equivClasses.values.stream.map(ImmutableSet.copyOf).iterator)
  }
}
