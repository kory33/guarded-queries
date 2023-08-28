package io.github.kory33.guardedqueries.core.testcases

/**
 * A static class containing test cases of GTGD rules and conjunctive queries.
 */
object GTGDRuleAndConjunctiveQueryTestCases {
  private val conjunctiveQuery = TestCaseParsers.conjunctiveQuery

  object SimpleArity2Rule_0 {
    val nonReducibleJoinQuery: GTGDRuleAndConjunctiveQuery = GTGDRuleAndConjunctiveQuery(
      GTGDRuleSets.simpleArity2Rule_0,
      conjunctiveQuery.parse("EE y. R(z, y), R(y, w)")
    )

    val triangleBCQ: GTGDRuleAndConjunctiveQuery = GTGDRuleAndConjunctiveQuery(
      GTGDRuleSets.simpleArity2Rule_0,
      conjunctiveQuery.parse("EE x,y,z. R(x, y), R(y, z), R(z, x)")
    )

    val triangleBCQWithLeaf: GTGDRuleAndConjunctiveQuery = GTGDRuleAndConjunctiveQuery(
      GTGDRuleSets.simpleArity2Rule_0,
      conjunctiveQuery.parse("EE x,y,z. R(x, y), R(y, z), R(z, x), R(x, w)")
    )
  }
}
