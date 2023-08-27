package io.github.kory33.guardedqueries.core.subqueryentailments

import com.google.common.collect.ImmutableBiMap
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Variable

case class SubqueryEntailmentInstance(
  ruleConstantWitnessGuess: ImmutableMap[Variable, Constant],
  coexistentialVariables: ImmutableSet[Variable],
  localInstance: FormalInstance[LocalInstanceTerm],
  localWitnessGuess: ImmutableMap[Variable, LocalInstanceTerm.LocalName],
  queryConstantEmbedding: ImmutableBiMap[Constant, LocalInstanceTerm.LocalName]
) {
  def ruleConstantWitnessGuessAsMapToInstanceTerms
    : ImmutableMap[Variable, LocalInstanceTerm.RuleConstant] =
    MapExtensions.composeWithFunction(
      this.ruleConstantWitnessGuess,
      LocalInstanceTerm.RuleConstant(_)
    )

  def withLocalInstance(newLocalInstance: FormalInstance[LocalInstanceTerm]) =
    this.copy(localInstance =
      newLocalInstance
    )
}
