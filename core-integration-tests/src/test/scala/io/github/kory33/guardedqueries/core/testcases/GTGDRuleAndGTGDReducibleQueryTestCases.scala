package io.github.kory33.guardedqueries.core.testcases

import uk.ac.ox.cs.gsat.GTGD

/**
 * A static class containing test cases of GTGD rules and GTGD-reducible queries.
 */
object GTGDRuleAndGTGDReducibleQueryTestCases {
  private val conjunctiveQuery = TestCaseParsers.conjunctiveQuery
  private val gtgd = TestCaseParsers.gtgd

  object SimpleArity2Rule_0 {
    val atomicQuery: GTGDRuleAndGTGDReducibleQuery = GTGDRuleAndGTGDReducibleQuery(
      GTGDRuleSets.simpleArity2Rule_0,
      GTGDReducibleConjunctiveQuery(
        conjunctiveQuery.parse("R(z_1, z_1)"),
        Set.empty,
        conjunctiveQuery.parse("R(z_1, z_1)")
      )
    )
    val joinQuery: GTGDRuleAndGTGDReducibleQuery = GTGDRuleAndGTGDReducibleQuery(
      GTGDRuleSets.simpleArity2Rule_0,
      GTGDReducibleConjunctiveQuery(
        conjunctiveQuery.parse("R(z_1, z_2), R(z_2, z_3)"),
        Set.empty,
        conjunctiveQuery.parse("R(z_1, z_2), R(z_2, z_3)")
      )
    )
    val existentialGuardedQuery_0: GTGDRuleAndGTGDReducibleQuery =
      GTGDRuleAndGTGDReducibleQuery(
        GTGDRuleSets.simpleArity2Rule_0,
        GTGDReducibleConjunctiveQuery(
          conjunctiveQuery.parse("EE y. R(x, y), R(y, y)"),
          Set(gtgd.parse("R(x, y), R(y, y) -> Goal(x)")),
          conjunctiveQuery.parse("Goal(x)")
        )
      )
    val existentialJoinQuery_0: GTGDRuleAndGTGDReducibleQuery = GTGDRuleAndGTGDReducibleQuery(
      GTGDRuleSets.simpleArity2Rule_0,
      GTGDReducibleConjunctiveQuery(
        conjunctiveQuery.parse("EE y,z. R(w, y), R(y, z)"),
        Set(gtgd.parse("R(y, z) -> I(y)"), gtgd.parse("R(w, y), I(y) -> Goal(w)")),
        conjunctiveQuery.parse("Goal(w)")
      )
    )
    val existentialJoinQuery_1: GTGDRuleAndGTGDReducibleQuery = GTGDRuleAndGTGDReducibleQuery(
      GTGDRuleSets.simpleArity2Rule_0,
      GTGDReducibleConjunctiveQuery(
        conjunctiveQuery.parse("EE y,z. R(y, t), R(t, w), R(w, z), U(z)"),
        Set(
          gtgd.parse("R(y, t) -> I_1(t)"),
          gtgd.parse("R(w, z), U(z) -> I_2(w)")
        ),
        conjunctiveQuery.parse("I_1(t), R(t, w), I_2(w)")
      )
    )
  }

  object ConstantRule {
    val atomicQuery: GTGDRuleAndGTGDReducibleQuery = GTGDRuleAndGTGDReducibleQuery(
      GTGDRuleSets.constantRule,
      GTGDReducibleConjunctiveQuery(
        conjunctiveQuery.parse("R(c_3, x)"),
        Set.empty,
        conjunctiveQuery.parse("R(c_3, x)")
      )
    )
    val existentialBooleanQueryWithConstant: GTGDRuleAndGTGDReducibleQuery =
      GTGDRuleAndGTGDReducibleQuery(
        GTGDRuleSets.constantRule,
        GTGDReducibleConjunctiveQuery(
          conjunctiveQuery.parse("EE y. R(c_3, y)"),
          Set(gtgd.parse("R(c_3, y) -> Goal()")),
          conjunctiveQuery.parse("Goal()")
        )
      )
    val existentialGuardedWithConstant: GTGDRuleAndGTGDReducibleQuery =
      GTGDRuleAndGTGDReducibleQuery(
        GTGDRuleSets.constantRule,
        GTGDReducibleConjunctiveQuery(
          conjunctiveQuery.parse("EE y. R(c_1, y), R(y, w), R(w, c_3)"),
          Set(gtgd.parse("R(c_1, y), R(y, w), R(w, c_3) -> Goal(w)")),
          conjunctiveQuery.parse("Goal(w)")
        )
      )
  }

  object Arity3Rule_0 {
    // WARNING: This particular query takes too much time + heap space to rewrite using NaiveDPTableSEEnumeration / NormalizingDPTableSEEnumeration
    val existentialJoinQuery: GTGDRuleAndGTGDReducibleQuery = GTGDRuleAndGTGDReducibleQuery(
      GTGDRuleSets.arity3Rule_0,
      GTGDReducibleConjunctiveQuery(
        conjunctiveQuery.parse("EE y. T(x, y, z), T(y, x, z)"),
        Set(gtgd.parse("T(x, y, z), T(y, x, z) -> Goal(x, z)")),
        conjunctiveQuery.parse("Goal(x, z)")
      )
    )
  }

  object Arity3Rule_1 {
    val existentialGuardedQuery: GTGDRuleAndGTGDReducibleQuery = GTGDRuleAndGTGDReducibleQuery(
      GTGDRuleSets.arity3Rule_1,
      GTGDReducibleConjunctiveQuery(
        conjunctiveQuery.parse("EE x. T(x, y, z), U(x)"),
        Set(gtgd.parse("TT(x, y, z), U(x) -> Goal(y, z)")),
        conjunctiveQuery.parse("Goal(y, z)")
      )
    )
  }

  object Arity4Rule {
    val atomicQuery: GTGDRuleAndGTGDReducibleQuery = GTGDRuleAndGTGDReducibleQuery(
      GTGDRuleSets.arity4Rule,
      GTGDReducibleConjunctiveQuery(
        conjunctiveQuery.parse("S(x, z, y, x)"),
        Set.empty,
        conjunctiveQuery.parse("S(x, z, y, x)")
      )
    )

    // WARNING: This particular query takes too much time + heap space to rewrite using NaiveDPTableSEEnumeration / NormalizingDPTableSEEnumeration
    val existentialGuardedQuery_0: GTGDRuleAndGTGDReducibleQuery =
      GTGDRuleAndGTGDReducibleQuery(
        GTGDRuleSets.arity4Rule,
        GTGDReducibleConjunctiveQuery(
          conjunctiveQuery.parse("EE y. S(x, y, y, z), R(y, x)"),
          Set(gtgd.parse("S(x, y, y, z), R(y, x) -> Goal(x, z)")),
          conjunctiveQuery.parse("Goal(x, z)")
        )
      )

    // WARNING: This particular query takes too much time + heap space to rewrite using NaiveDPTableSEEnumeration / NormalizingDPTableSEEnumeration
    val existentialJoinQuery_0: GTGDRuleAndGTGDReducibleQuery = GTGDRuleAndGTGDReducibleQuery(
      GTGDRuleSets.arity4Rule,
      GTGDReducibleConjunctiveQuery(
        conjunctiveQuery.parse("EE y_1, y_2. T(z_1, y_1, z_2), R(y_1, y_2), T(y_1, y_2, z_2)"),
        Set(
          gtgd.parse("R(y_1, y_2), T(y_1, y_2, z_2) -> I(y_1, z_2)"),
          gtgd.parse("T(z_1, y_1, z_2), I(y_1, z_2) -> Goal(z_1, z_2)")
        ),
        conjunctiveQuery.parse("Goal(z_1, z_2)")
      )
    )
  }
}
