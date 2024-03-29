package io.github.kory33.guardedqueries.core.rewriting

import io.github.kory33.guardedqueries.core.datalog.DatalogProgram
import io.github.kory33.guardedqueries.core.datalog.GuardedDatalogProgram
import uk.ac.ox.cs.gsat.AbstractSaturation
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Formula

import scala.jdk.CollectionConverters.*

class SaturatedRuleSet[+RuleClass <: GTGD](
  saturation: AbstractSaturation[? <: GTGD],
  originalRules: Set[RuleClass]
) {
  val saturatedRules: Set[GTGD] = saturation.run(originalRules.toList.asJava).asScala.toSet

  val saturatedRulesAsDatalogProgram: DatalogProgram =
    DatalogProgram.tryFromDependencies(saturatedRules)

  val saturatedRulesAsGuardedDatalogProgram: GuardedDatalogProgram =
    GuardedDatalogProgram.tryFromDependencies(saturatedRules)

  val existentialRules: Iterable[RuleClass] =
    originalRules.view.filter(rule => rule.getExistential.length > 0).toSet

  val allRules: Set[GTGD] = saturatedRules ++ existentialRules

  lazy val constants: Set[Constant] = allRules.flatMap(SaturatedRuleSet.constantsInFormula)
}

object SaturatedRuleSet {
  private def constantsInFormula(formula: Formula): Set[Constant] =
    formula.getTerms.collect { case constant: Constant => constant }.toSet
}
