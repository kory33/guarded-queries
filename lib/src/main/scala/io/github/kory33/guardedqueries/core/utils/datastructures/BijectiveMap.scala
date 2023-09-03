package io.github.kory33.guardedqueries.core.utils.datastructures

/**
 * An object that represents a bijective map between a `Set[K]` and a `Set[V]`.
 *
 * @param forwardMap
 *   an injective map from keys to values, i.e. a map such that for all pairs `k1, k2` of values
 *   of `K`, `forward.get(k1) == forward.get(k2)` if and only if `k1 == k2`
 * @param inverseMap
 *   the inverse of `forward`, i.e. a map satisfying `inverse(forward(k)) == k` and
 *   `forward(inverse(v)) == v` for all `k` in the domain of `forward` and `v` in the range of
 *   `forward`
 */
class BijectiveMap[K, V] private /* unchecked constructor */ (
  forwardMap: Map[K, V],
  inverseMap: Map[V, K]
) {
  def inverse: BijectiveMap[V, K] = new BijectiveMap(inverseMap, forwardMap)

  def asMap: Map[K, V] = forwardMap

  def values: Set[V] = inverseMap.keySet

  // We essentially delegate methods to `forwardMap` by defining `equals`, `hashCode`
  // and `toString` using `forwardMap`, and by providing a `Conversion`
  // from `BijectiveMap` to `Map` via `forwardMap`.

  override def equals(obj: Any): Boolean = obj match {
    case that: BijectiveMap[?, ?] => this.asMap == that.asMap
    case _                        => false
  }

  override def hashCode(): Int = asMap.hashCode()

  override def toString: String = s"BijectiveMap(${asMap.toString})"
}

object BijectiveMap {

  given [K, V]: Conversion[BijectiveMap[K, V], Map[K, V]] = _.asMap

  given Extensions: AnyRef with
    extension [K, V](bmap: BijectiveMap[K, V])
      def restrictToKeys(keys: Set[K]): BijectiveMap[K, V] =
        import io.github.kory33.guardedqueries.core.utils.extensions.MapExtensions.given
        // this call to .get never throws since a restriction of an injective map is again injective
        BijectiveMap.tryFromInjectiveMap(bmap.asMap.restrictToKeys(keys)).get

  /**
   * Construct a bijective map from an injective map. If the given map is not injective, this
   * method returns a `None`.
   */
  def tryFromInjectiveMap[K, V](forward: Map[K, V]): Option[BijectiveMap[K, V]] = {
    val inverse = forward.map { case (k, v) => (v, k) }

    if (forward.size == inverse.size) {
      // The map is injective if and only if `forward` and `inverse` have the same size,
      // since the definition of `inverse` "collapses" all entries in `forward`
      // with the same values into one entry.
      Some(new BijectiveMap(forward, inverse))
    } else {
      None
    }
  }

  def empty[K, V]: BijectiveMap[K, V] = tryFromInjectiveMap(Map.empty[K, V]).get
}
