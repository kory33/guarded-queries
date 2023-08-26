package io.github.kory33.guardedqueries.core.fol

import uk.ac.ox.cs.pdq.fol._

/**
 * A class of Datalog rules.
 *
 * A Datalog rule is a Dependency such that
 *   - all variables are universally quantified, and
 *   - every variable in the head appears in some atom in the body.
 */
object DatalogRule {
  private def notConjunctionOfAtoms(formula: Formula): Boolean = formula match
    case _: Atom                  => false
    case conjunction: Conjunction => conjunction.getChildren.exists(notConjunctionOfAtoms)
    case _                        => true

  /**
   * Attempt to regard a given dependency as a Datalog rule.
   *
   * This method may fail with an [[IllegalArgumentException]] if the given dependency has
   * existential variables in the head, or if proper subformulae are not conjunctions of atoms.
   */
  def tryFromDependency(dependency: Dependency): DatalogRule = {
    val body = dependency.getBody
    val head = dependency.getHead
    if (notConjunctionOfAtoms(body)) throw new IllegalArgumentException(
      "Body of a DatalogRule must be a conjunction of atoms, got" + body.toString
    )
    if (notConjunctionOfAtoms(head)) throw new IllegalArgumentException(
      "Head of a DatalogRule must be a conjunction of atoms, got" + head.toString
    )
    new DatalogRule(dependency.getBodyAtoms, dependency.getHeadAtoms)
  }
}

class DatalogRule(body: Array[Atom], head: Array[Atom]) extends TGD(body, head) {
  if (this.getExistential.length != 0) throw new IllegalArgumentException(
    "Datalog rule cannot contain existential variables, got " + super.toString
  )
}
