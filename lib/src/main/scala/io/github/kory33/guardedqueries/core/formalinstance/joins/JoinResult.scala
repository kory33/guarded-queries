package io.github.kory33.guardedqueries.core.formalinstance.joins

import com.google.common.collect.ImmutableList
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable
import java.util
import java.util.function.Function

/**
 * A class of objects representing the result of a join operation. <p> We say that a {@code
 * JoinResult} is well-formed if: <ol> <li>the {@code variableOrdering} is a list of distinct
 * variables</li> <li>each list in {@code orderedMapping} has the same length as {@code
 * variableOrdering}</li> <li>{@code orderedMapping} is a list opf distinct join result
 * tuples</li> </ol> <p> For example, given a query {@code Q(x, y), R(y, z)}, a {@code
 * JoinResult} of the form <ol> <li>{@code variableOrdering = [x, y, z]}</li> <li>{@code
 * orderedMapping = [[a, b, c], [a, c, d]]}</li> </ol> is a well-formed answer to the query,
 * insinuating that {@code Q(a, b), R(b, c)} and {@code Q(a, c), R(c, d)} are the only answers
 * to the query.
 *
 * @param <Term>
 *   type of values (typically constants) that appear in the input instance of the join
 *   operation
 */
class JoinResult[Term](
  variableOrdering: ImmutableList[Variable],
  orderedMappingsOfAllHomomorphisms: ImmutableList[ImmutableList[Term]]
) {
  // invariant: a single instance of ImmutableList<Variable> variableOrdering is shared among
  //            all HomomorphicMapping objects
  final var allHomomorphisms: ImmutableList[HomomorphicMapping[Term]] =
    ImmutableList.copyOf(orderedMappingsOfAllHomomorphisms.stream.map(
      (homomorphism: ImmutableList[Term]) =>
        new HomomorphicMapping[Term](variableOrdering, homomorphism)
    ).iterator)

  /**
   * Materialize the given atom by replacing the variables in the atom with the values in this
   * result. <p> The returned list has the same length as {@code orderedMapping}.
   *
   * @param atomWhoseVariablesAreInThisResult
   *   a function-free atom whose variables are covered by this join result
   * @param constantInclusion
   *   a function that maps a constant in the input instance to a term
   * @return
   *   a list of formal facts that are the result of materializing the given atom
   */
  def materializeFunctionFreeAtom(atomWhoseVariablesAreInThisResult: Atom,
                                  constantInclusion: Function[Constant, Term]
  ): ImmutableList[FormalFact[Term]] =
    ImmutableList.copyOf(this.allHomomorphisms.stream.map((h: HomomorphicMapping[Term]) =>
      h.materializeFunctionFreeAtom(atomWhoseVariablesAreInThisResult, constantInclusion)
    ).iterator)

  /**
   * Extend the join result by adjoining a constant homomorphism. <p> For instance, suppose that
   * this join result is obtained as an answer to a query {@code Q(x, y), R(y, z)} and has the
   * following data: <ol> <li>{@code allHomomorphisms = [{x -> a, y -> b, z -> c}, {x -> a, y ->
   * c, z -> d}]}</li> </ol> We can "extend" these results by adjoining a constant homomorphism
   * {@code {w -> e}}. The result of such operation is: <ol> <li>{@code allHomomorphisms = [{x
   * -> a, y -> b, z -> c, w -> e}, {x -> a, y -> c, z -> d, w -> e}]}</li> </ol>
   *
   * @param constantHomomorphism
   *   The homomorphism with which the join result is to be extended
   * @return
   *   the extended join result
   * @throws IllegalArgumentException
   *   if the given homomorphism maps a variable in {@code variableOrdering}
   */
  def extendWithConstantHomomorphism(constantHomomorphism: util.Map[Variable, Term])
    : JoinResult[Term] = {
    if (allHomomorphisms.isEmpty) {
// then this join result contains no information and there is nothing to extend
      return this
    }
    val variableOrdering = allHomomorphisms.get(0).variableOrdering
    if (variableOrdering.stream.anyMatch(constantHomomorphism.containsKey))
      throw new IllegalArgumentException(
        "The given constant homomorphism has a conflicting variable mapping"
      )
    val extensionVariableOrdering = ImmutableList.copyOf(constantHomomorphism.keySet)
    val extensionMapping = ImmutableList.copyOf(
      extensionVariableOrdering.stream.map(constantHomomorphism.get).iterator
    )
    val extendedVariableOrdering = ImmutableList.builder[Variable].addAll(
      variableOrdering
    ).addAll(extensionVariableOrdering).build
    val extendedHomomorphisms = allHomomorphisms.stream.map(
      (homomorphism: HomomorphicMapping[Term]) =>
        ImmutableList.builder[Term].addAll(homomorphism.orderedMapping).addAll(
          extensionMapping
        ).build
    )
    new JoinResult[Term](
      extendedVariableOrdering,
      ImmutableList.copyOf(extendedHomomorphisms.iterator)
    )
  }
}
