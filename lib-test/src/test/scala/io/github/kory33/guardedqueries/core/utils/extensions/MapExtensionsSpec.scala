package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*

object MapExtensionsSpec extends Properties("MapExtensions") {
  import Prop.forAll

  override def overrideParameters(p: Test.Parameters): Test.Parameters = p.withMinSuccessfulTests(1000)

  property("composeWithFunction should be equivalent to Scala's view.mapValues.toMap") = forAll { (xs: Map[String, Int]) =>
    MapExtensions.composeWithFunction(xs.asJava, (x: Int) => x * 3).asScala == xs.view.mapValues(_ * 3).toMap
  }

  property("preimages(map, ys): every entry in output should have a value all of whose elements are mapped to the key by map") =
    forAll { (map: Map[String, Int], ys: Set[Int]) =>
      MapExtensions.preimages(map.asJava, ys.asJava).asScala.forall { case (key, value) =>
        value.asScala.forall(map(_) == key)
      }
    }

  property("preimages(map, ys): if a value is mapped by map to some value in the range ys, it must appear in some value in the output map") =
    forAll { (map: Map[String, Int], ys: Set[Int]) =>
      val preimageMap = MapExtensions.preimages(map.asJava, ys.asJava).asScala

      map.keySet
        .filter(value => ys.contains(map(value)))
        .forall { value =>
          preimageMap.exists { case (_, values) =>
            values.asScala.contains(value)
          }
        }
    }
  
  property("restrictToKeys should be equivalent to Scala's filterKeys(contains).toMap") =
    forAll { (xs: Map[Int, String], ys: Set[Int]) =>
      MapExtensions.restrictToKeys(xs.asJava, ys.asJava).asScala == xs.view.filterKeys(ys.contains).toMap
    }
}
