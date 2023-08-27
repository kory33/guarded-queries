package io.github.kory33.guardedqueries.core.rewriting

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import io.github.kory33.guardedqueries.core.datalog.DatalogProgram
import uk.ac.ox.cs.gsat.AbstractSaturation
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.{Constant, Dependency, Formula}

import java.util
import java.util.stream.Stream

class SaturatedRuleSet[RuleClass <: GTGD](
  saturation: AbstractSaturation[_ <: GTGD],
  originalRules: util.Collection[_ <: RuleClass]
) {

  val saturatedRules: ImmutableList[GTGD] =
    ImmutableList.copyOf(saturation.run(new util.ArrayList[Dependency](originalRules)))

  val saturatedRulesAsDatalogProgram: DatalogProgram =
    DatalogProgram.tryFromDependencies(this.saturatedRules)

  val existentialRules: ImmutableList[RuleClass] =
    ImmutableList.copyOf(originalRules.stream.filter((rule: _$1) =>
      rule.getExistential.length > 0
    ).iterator)

  val allRules: ImmutableList[GTGD] = {
    val allRulesBuilder: ImmutableList.Builder[GTGD] = ImmutableList.builder[GTGD]
    allRulesBuilder.addAll(existentialRules)
    allRulesBuilder.addAll(saturatedRules)
    allRulesBuilder.build
  }

  lazy val constants: ImmutableSet[Constant] = ImmutableSet.copyOf(
    this.allRules.stream.flatMap(SaturatedRuleSet.constantsInFormula).iterator
  )
}

object SaturatedRuleSet {
  private def constantsInFormula(formula: Formula) =
    StreamExtensions.filterSubtype(util.Arrays.stream(formula.getTerms), classOf[Constant])
}
