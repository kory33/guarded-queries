package io.github.kory33.guardedqueries.core.subqueryentailments

import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.utils.datastructures.BijectiveMap
import io.github.kory33.guardedqueries.core.utils.extensions.MapExtensions
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable

case class SubqueryEntailmentInstance(
  ruleConstantWitnessGuess: Map[Variable, Constant],
  coexistentialVariables: Set[Variable],
  localInstance: FormalInstance[LocalInstanceTerm],
  localWitnessGuess: Map[Variable, LocalInstanceTerm.LocalName],
  queryConstantEmbedding: BijectiveMap[Constant, LocalInstanceTerm.LocalName]
) {
  def ruleConstantWitnessGuessAsMapToInstanceTerms
    : Map[Variable, LocalInstanceTerm.RuleConstant] =
    ruleConstantWitnessGuess
      .view.mapValues(LocalInstanceTerm.RuleConstant(_))
      .toMap

  def withLocalInstance(newLocalInstance: FormalInstance[LocalInstanceTerm]) =
    this.copy(localInstance =
      newLocalInstance
    )
}
