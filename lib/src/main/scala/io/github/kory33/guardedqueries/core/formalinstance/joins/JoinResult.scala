package io.github.kory33.guardedqueries.core.formalinstance.joins

import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable

//format: off
/**
 * A class of objects representing the result of a join operation.
 *
 * We say that a `JoinResult` is well-formed if: <ol> <li>the `variableOrdering` is a list of
 * distinct variables</li> <li>each list in `orderedMapping` has the same length as
 * `variableOrdering`</li> <li>`orderedMapping` is a list opf distinct join result tuples</li>
 * </ol>
 *
 * For example, given a query `Q(x, y), R(y, z)`, a `JoinResult` of the form
 *
 * <ol> <li>`variableOrdering = [x, y, z]`</li>
 *
 * <li> `orderedMapping = [ [a, b, c], [a, c, d] ]` </li> </ol>
 *
 * is a well-formed answer to the query, insinuating that `Q(a, b), R(b, c)` and
 * `Q(a, c), R(c,d)` are the only answers to the query.
 *
 * @tparam Term
 *   type of values (typically constants) that appear in the input instance of the join
 *   operation
 */
//format: on
class JoinResult[Term](
  variableOrdering: List[Variable],
  orderedMappingsOfAllHomomorphisms: List[List[Term]]
) {
  // invariant: a single instance of List<Variable> variableOrdering is shared among
  //            all HomomorphicMapping objects
  final var allHomomorphisms: List[HomomorphicMapping[Term]] =
    orderedMappingsOfAllHomomorphisms.map((homomorphism: List[Term]) =>
      HomomorphicMapping[Term](variableOrdering, homomorphism)
    )

  /**
   * Materialize the given atom by replacing the variables in the atom with the values in this
   * result.
   *
   * The returned list has the same length as `orderedMapping`.
   *
   * @param atomWhoseVariablesAreInThisResult
   *   a function-free atom whose variables are covered by this join result
   * @param constantInclusion
   *   a function that maps a constant in the input instance to a term
   * @return
   *   a list of formal facts that are the result of materializing the given atom
   */
  def materializeFunctionFreeAtom(atomWhoseVariablesAreInThisResult: Atom,
                                  constantInclusion: Constant => Term
  ): List[FormalFact[Term]] =
    allHomomorphisms.map((h: HomomorphicMapping[Term]) =>
      h.materializeFunctionFreeAtom(atomWhoseVariablesAreInThisResult, constantInclusion)
    )

  // format: off
  /**
   * Extend the join result by adjoining a constant homomorphism.
   *
   * For instance, suppose that this join result is obtained as an answer to a query
   * `Q(x, y), R(y, z)` and has the following data:
   * <ol>
   *   <li>`allHomomorphisms = [{x -> a, y -> b, z -> c}, {x -> a, y -> c, z -> d}]}`</li>
   * </ol>
   *
   * We can "extend" these results by adjoining a constant homomorphism `{w -> e`}.
   * The result of such operation is:
   * <ol>
   *  <li>`allHomomorphisms = [{x -> a, y -> b, z -> c, w -> e, {x -> a, y -> c, z -> d, w -> e}]`</li>
   * </ol>
   *
   * @param constantHomomorphism
   *   The homomorphism with which the join result is to be extended
   * @return
   *   the extended join result
   * @throws IllegalArgumentException
   *   if the given homomorphism maps a variable in `variableOrdering`
   */
  // format: on
  def extendWithConstantHomomorphism(constantHomomorphism: Map[Variable, Term])
    : JoinResult[Term] = {
    if (allHomomorphisms.isEmpty) {
      // then this join result contains no information and there is nothing to extend
      return this
    }
    val variableOrdering = allHomomorphisms.head.variableOrdering
    if (variableOrdering.intersects(constantHomomorphism.keySet))
      throw IllegalArgumentException(
        "The given constant homomorphism has a conflicting variable mapping"
      )

    val extensionVariableOrdering = constantHomomorphism.keySet.toList
    val extensionMapping = extensionVariableOrdering.map(constantHomomorphism)

    val extendedVariableOrdering = variableOrdering ++ extensionVariableOrdering
    val extendedHomomorphisms = allHomomorphisms.map(_.orderedMapping ++ extensionMapping)

    JoinResult[Term](extendedVariableOrdering, extendedHomomorphisms)
  }
}
