package io.github.kory33.guardedqueries.core.utils.extensions

import org.scalacheck.*
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.*

import scala.jdk.CollectionConverters.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class MapExtensionsSpec extends AnyFlatSpec with ScalaCheckPropertyChecks {

  "every entry in .preimages(map, ys)" should "have a value all of whose elements are mapped to the key by map" in {
    forAll(minSuccessful(1000)) { (map: Map[String, Int], ys: Set[Int]) =>
      assert {
        MapExtensions.preimages(map, ys).forall {
          case (key, value) =>
            value.forall(map(_) == key)
        }
      }
    }
  }

  "every value mapped by map to some value in the range ys" should "appear in some value in preimages(map, ys)" in {
    forAll(minSuccessful(1000)) { (map: Map[String, Int], ys: Set[Int]) =>
      val preimageMap = MapExtensions.preimages(map, ys)

      assert {
        map.keySet
          .filter(value => ys.contains(map(value)))
          .forall { value =>
            preimageMap.exists {
              case (_, values) =>
                values.contains(value)
            }
          }
      }
    }
  }
}
