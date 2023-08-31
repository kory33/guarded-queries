package io.github.kory33.guardedqueries.core.formalinstance

import uk.ac.ox.cs.pdq.fol.Constant

/**
 * A typeclass for term alphabet specifying that the alphabet includes constants of first-order
 * logic.
 */
trait IncludesFolConstants[TA] {
  def includeConstant(constant: Constant): TA
}

object IncludesFolConstants {
  given IncludesFolConstants[Constant] with {
    override def includeConstant(constant: Constant): Constant = constant
  }

  def apply[TA](using ev: IncludesFolConstants[TA]): ev.type = ev
}
