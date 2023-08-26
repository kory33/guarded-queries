package io.github.kory33.guardedqueries.core.formalinstance.joins

import com.google.common.collect.ImmutableList
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable
import java.util
import java.util.function.Function

/**
 * A mapping from variables to terms, which is part of a homomorphism mapping a query to an
 * instance. <p> {@code variableOrdering} must be an ordered list of variables to be mapped, and
 * this must not contain any duplicate variables (no runtime check is enforced for this). The
 * {@code i}-th element of {@code orderedMapping} specifies to which term the {@code i}-th
 * variable in {@code variableOrdering} is mapped. <p> For instance, suppose that {@code
 * variableOrdering = [x, y, z]} and {@code orderedMapping = [a, a, b]}. Then the mapping this
 * object represents is {@code x -> a, y -> a, z -> b}.
 */
case class HomomorphicMapping[Term](
  variableOrdering: ImmutableList[Variable],
  orderedMapping: ImmutableList[Term]
) extends Function[Variable, Term] {
  if (variableOrdering.size != orderedMapping.size) throw new IllegalArgumentException(
    "variableOrdering and orderedMapping must have the same size"
  )

  /**
   * Returns the term to which the given variable is mapped.
   *
   * @param variable
   *   a variable in {@code variableOrdering}
   * @return
   *   the term to which the given variable is mapped
   * @throws IllegalArgumentException
   *   if the given variable is not in {@code variableOrdering}
   */
  override def apply(variable: Variable) = {
    val index = variableOrdering.indexOf(variable)
    if (index == -1) throw new IllegalArgumentException("variable is not in variableOrdering")
    orderedMapping.get(index)
  }

  /**
   * Materialize the given atom by mapping the variables in the atom into terms specified by
   * this homomorphic mapping.
   *
   * @param atomWhoseVariablesAreInThisResult
   *   a function-free atom whose variables are in {@code variableOrdering}
   * @param constantInclusion
   *   a function that maps a constant in the input instance to a term
   */
  def materializeFunctionFreeAtom(
    atomWhoseVariablesAreInThisResult: Atom,
    constantInclusion: Function[Constant, Term]
  ) = {
    val inputAtomAsFormalFact = FormalFact.fromAtom(atomWhoseVariablesAreInThisResult)
    inputAtomAsFormalFact.map((term: Term) => {
      if (term.isInstanceOf[Constant]) constantInclusion.apply(constant)
      else if (term.isInstanceOf[Variable]) this.apply(variable)
      else
        throw new IllegalArgumentException("Term " + term + " is neither constant nor variable")

    })
  }

  /**
   * Materialize the given set of atoms by applying {@link #materializeFunctionFreeAtom(Atom,
   * Function)} to each atom.
   *
   * @param atomsWhoseVariablesAreInThisResult
   *   a set of function-free atoms whose variables are in {@code variableOrdering}
   * @param constantInclusion
   *   a function that maps a constant in the input instance to a term
   */
  def materializeFunctionFreeAtoms(
    atomsWhoseVariablesAreInThisResult: util.Collection[Atom],
    constantInclusion: Function[Constant, Term]
  ) = FormalInstance.fromIterator(atomsWhoseVariablesAreInThisResult.stream.map((atom: Atom) =>
    this.materializeFunctionFreeAtom(atom, constantInclusion)
  ).iterator)

  /**
   * Extend the homomorphism with the given mapping.
   *
   * @throws IllegalArgumentException
   *   if the given homomorphism maps a variable in {@code variableOrdering}
   */
  def extendWithMapping(
    additionalMapping: util.Map[Variable, Term]
  ): HomomorphicMapping[Term] = {
    if (additionalMapping.isEmpty) {
      // there is nothing to extend
      return this
    }

    if (variableOrdering.stream.anyMatch(additionalMapping.containsKey))
      throw new IllegalArgumentException(
        s"additionalMapping $additionalMapping contains a variable in variableOrdering $variableOrdering"
      )

    val newVariableOrdering = ImmutableList.builder[Variable].addAll(variableOrdering).addAll(
      additionalMapping.keySet
    ).build

    val newMappingSize = newVariableOrdering.size
    val newOrderedMapping = ImmutableList.builder[Term]
    for (index <- 0 until newMappingSize) {
      if (index < orderedMapping.size) newOrderedMapping.add(orderedMapping.get(index))
      else newOrderedMapping.add(additionalMapping.get(newVariableOrdering.get(index)))
    }
    new HomomorphicMapping[Term](newVariableOrdering, newOrderedMapping.build)
  }
}
