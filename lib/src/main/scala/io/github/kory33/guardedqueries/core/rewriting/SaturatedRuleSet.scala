package io.github.kory33.guardedqueries.core.rewriting

import io.github.kory33.guardedqueries.core.datalog.DatalogProgram
import uk.ac.ox.cs.gsat.AbstractSaturation
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.{Constant, Dependency, Formula}

import java.util
import java.util.stream.Stream
import io.github.kory33.guardedqueries.core.utils.extensions.StreamExtensions

class SaturatedRuleSet[RuleClass <: GTGD](
  saturation: AbstractSaturation[_ <: GTGD],
  originalRules: util.Collection[_ <: RuleClass]
) {

  val saturatedRules: List[GTGD] =
    List.copyOf(saturation.run(new util.ArrayList[Dependency](originalRules)))

  val saturatedRulesAsDatalogProgram: DatalogProgram =
    DatalogProgram.tryFromDependencies(this.saturatedRules)

  val existentialRules: List[RuleClass] =
    List.copyOf(originalRules.stream.filter(rule =>
      rule.getExistential.length > 0
    ).iterator)

  val allRules: List[GTGD] = {
    val allRulesBuilder: List.Builder[GTGD] = List.builder[GTGD]
    allRulesBuilder.addAll(existentialRules)
    allRulesBuilder.addAll(saturatedRules)
    allRulesBuilder.build
  }

  lazy val constants: Set[Constant] = Set.copyOf(
    this.allRules.stream.flatMap(SaturatedRuleSet.constantsInFormula).iterator
  )
}

object SaturatedRuleSet {
  private def constantsInFormula(formula: Formula) =
    StreamExtensions.filterSubtype(util.Arrays.stream(formula.getTerms), classOf[Constant])
}
