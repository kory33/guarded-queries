package io.github.kory33.guardedqueries.core.formalinstance.joins

import io.github.kory33.guardedqueries.core.formalinstance.{
  FormalFact,
  FormalInstance,
  IncludesFolConstants
}
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable
import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given

import scala.collection.mutable.ArrayBuffer

/**
 * A mapping from variables to terms, which is part of a homomorphism mapping a query to an
 * instance.
 *
 * `variableOrdering` must be an ordered list of variables to be mapped, and this must not
 * contain any duplicate variables (no runtime check is enforced for this). The `i`-th element
 * of `orderedMapping` specifies to which term the `i`-th variable in `variableOrdering` is
 * mapped.
 *
 * For instance, suppose that `variableOrdering = [x, y, z]` and {@code orderedMapping = [a, a,
 * b]}. Then the mapping this object represents is `x -> a, y -> a, z -> b`.
 */
case class HomomorphicMapping[Term](
  variableOrdering: List[Variable],
  orderedMapping: List[Term]
) extends (Variable => Term) {
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
  override def apply(variable: Variable) = {
    val index = variableOrdering.indexOf(variable)
    if (index == -1) throw new IllegalArgumentException("variable is not in variableOrdering")
    orderedMapping(index)
  }

  /**
   * Materialize the given atom by mapping the variables in the atom into terms specified by
   * this homomorphic mapping.
   *
   * @param atomWhoseVariablesAreInThisResult
   *   a function-free atom whose variables are in `variableOrdering`
   * @param constantInclusion
   *   a function that maps a constant in the input instance to a term
   */
  def materializeFunctionFreeAtom(
    atomWhoseVariablesAreInThisResult: Atom
  )(using IncludesFolConstants[Term]) = {
    FormalFact.fromAtom(atomWhoseVariablesAreInThisResult).map({
      case constant: Constant => IncludesFolConstants[Term].includeConstant(constant)
      case variable: Variable => this.apply(variable)
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
  )(using IncludesFolConstants[Term]) =
    FormalInstance(atomsWhoseVariablesAreInThisResult.map(materializeFunctionFreeAtom))

  /**
   * Extend the homomorphism with the given mapping.
   *
   * @throws IllegalArgumentException
   *   if the given homomorphism maps a variable in `variableOrdering`
   */
  def extendWithMapping(additionalMapping: Map[Variable, Term]): HomomorphicMapping[Term] = {
    if (additionalMapping.isEmpty) {
      // there is nothing to extend
      return this
    }

    if (variableOrdering.toSet.intersects(additionalMapping.keySet))
      throw new IllegalArgumentException(
        s"additionalMapping $additionalMapping contains a variable in variableOrdering $variableOrdering"
      )

    val newVariableOrdering = variableOrdering.toList ++ additionalMapping.keySet

    val newMappingSize = newVariableOrdering.size
    val newOrderedMapping = ArrayBuffer.empty[Term]
    for (index <- 0 until newMappingSize) {
      if (index < orderedMapping.size) newOrderedMapping.append(orderedMapping(index))
      else newOrderedMapping.append(additionalMapping(newVariableOrdering(index)))
    }
    new HomomorphicMapping[Term](newVariableOrdering, newOrderedMapping.toList)
  }
}
