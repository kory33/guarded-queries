package io.github.kory33.guardedqueries.core.formalinstance.joins

import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.IncludesFolConstants
import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable

/**
 * A mapping from variables to terms, which is part of a homomorphism mapping a query to an
 * instance.
 *
 * `variableOrdering` must be an ordered list of variables to be mapped, and this must not
 * contain any duplicate variables (no runtime check is enforced for this). The `i`-th element
 * of `orderedMapping` specifies to which term the `i`-th variable in `variableOrdering` is
 * mapped.
 *
 * For instance, suppose that `variableOrdering = [x, y, z]` and `orderedMapping = [a, a, b]`.
 * Then the mapping this object represents is `x -> a, y -> a, z -> b`.
 */
case class HomomorphicMapping[QueryVariable, Term](
  variableOrdering: List[QueryVariable],
  orderedMapping: List[Term]
) extends (QueryVariable => Term) {
  if (variableOrdering.size != orderedMapping.size) throw new IllegalArgumentException(
    "variableOrdering and orderedMapping must have the same size"
  )

  /**
   * Returns the term to which the given variable is mapped.
   *
   * @param variable
   *   a variable in `variableOrdering`
   * @return
   *   the term to which the given variable is mapped
   * @throws IllegalArgumentException
   *   if the given variable is not in `variableOrdering`
   */
  override def apply(variable: QueryVariable): Term = {
    val index = variableOrdering.indexOf(variable)
    if (index == -1) throw new IllegalArgumentException("variable is not in variableOrdering")
    orderedMapping(index)
  }

  /**
   * Returns a map from variables to terms, which is equivalent to this homomorphic mapping.
   */
  def toMap: Map[QueryVariable, Term] = variableOrdering.zip(orderedMapping).toMap

  /**
   * Materialize the given atom by mapping the variables in the atom into terms specified by
   * this homomorphic mapping.
   *
   * @param atomWhoseVariablesAreInThisResult
   *   a function-free atom whose variables are in `variableOrdering`
   */
  def materializeFunctionFreeAtom(
    atomWhoseVariablesAreInThisResult: Atom
  )(using i: IncludesFolConstants[Term], ev: QueryVariable =:= Variable): FormalFact[Term] = {
    FormalFact.fromAtom(atomWhoseVariablesAreInThisResult).map({
      case constant: Constant => IncludesFolConstants[Term].includeConstant(constant)
      case variable: Variable => this.apply(ev.flip(variable))
      case term =>
        throw new IllegalArgumentException(s"Term $term is neither constant nor variable")
    })
  }

  /**
   * Materialize the given set of atoms by applying {@link #materializeFunctionFreeAtom(Atom,
   * Function)} to each atom.
   *
   * @param atomsWhoseVariablesAreInThisResult
   *   a set of function-free atoms whose variables are in `variableOrdering`
   * @param constantInclusion
   *   a function that maps a constant in the input instance to a term
   */
  def materializeFunctionFreeAtoms(
    atomsWhoseVariablesAreInThisResult: Set[Atom]
  )(using i: IncludesFolConstants[Term], ev: QueryVariable =:= Variable) =
    FormalInstance(atomsWhoseVariablesAreInThisResult.map(materializeFunctionFreeAtom))

  /**
   * Extend the homomorphism with the given mapping.
   *
   * @throws IllegalArgumentException
   *   if the given homomorphism maps a variable in `variableOrdering`
   */
  def extendWithMap(extensionMap: Map[QueryVariable, Term])
    : HomomorphicMapping[QueryVariable, Term] = {
    if (variableOrdering.toSet.intersects(extensionMap.keySet))
      throw new IllegalArgumentException(
        s"additionalMapping $extensionMap contains a variable in variableOrdering $variableOrdering"
      )

    val (extensionVariables, extensionTerms) = extensionMap.unzip
    HomomorphicMapping(
      variableOrdering ++ extensionVariables,
      orderedMapping ++ extensionTerms
    )
  }
}
