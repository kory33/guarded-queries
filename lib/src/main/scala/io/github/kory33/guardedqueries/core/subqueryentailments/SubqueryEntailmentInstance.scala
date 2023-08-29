package io.github.kory33.guardedqueries.core.subqueryentailments

import com.google.common.collect.ImmutableBiMap
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable
import io.github.kory33.guardedqueries.core.utils.extensions.MapExtensions

case class SubqueryEntailmentInstance(
  ruleConstantWitnessGuess: Map[Variable, Constant],
  coexistentialVariables: Set[Variable],
  localInstance: FormalInstance[LocalInstanceTerm],
  localWitnessGuess: Map[Variable, LocalInstanceTerm.LocalName],
  queryConstantEmbedding: ImmutableBiMap[Constant, LocalInstanceTerm.LocalName]
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
