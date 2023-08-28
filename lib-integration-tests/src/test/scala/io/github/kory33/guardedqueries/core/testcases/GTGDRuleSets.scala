package io.github.kory33.guardedqueries.core.testcases

import com.google.common.collect.ImmutableList
import uk.ac.ox.cs.gsat.GTGD

/**
 * A static class containing GTGD rules.
 */
object GTGDRuleSets {
  private val gtgd = TestCaseParsers.gtgd

  /**
   * An arity-2 rule set, adapted from
   * https://github.com/KRR-Oxford/Guarded-saturation/blob/bde32223ae4bc8ce084d233e7eede5ed1021adc7/src/test/java/uk/ac/ox/cs/gsat/SimpleSatTest.java#L49-L51
   */
  val simpleArity2Rule_0: ImmutableList[GTGD] = ImmutableList.of(
    gtgd.parse("A(x_1) -> EE x_2. R(x_1, x_2)"),
    gtgd.parse("R(x_1, x_2) -> U(x_2)"),
    gtgd.parse("R(x_1, x_2), U(x_2) -> P(x_1)")
  )

  // A rule set containing constants
  val constantRule: ImmutableList[GTGD] = ImmutableList.of(
    gtgd.parse("R(x_1, c_1) -> EE y_1. R(c_1, y_1), R(y_1, x_1)"),
    gtgd.parse("R(c_1, x_1) -> R(x_1, c_1), P(x_1)")
  )

  // An arity-3 rule set
  val arity3Rule_0: ImmutableList[GTGD] = ImmutableList.of(
    gtgd.parse("T(x_1, x_2, x_2) -> EE y_1, y_2. T(x_2, y_1, y_2)"),
    gtgd.parse("T(x_1, x_2, x_3) -> EE y_1. T(x_1, x_2, y_1)"),
    gtgd.parse("T(x_1, x_2, x_3) -> T(x_3, x_2, x_1)")
  )

  /**
   * An arity-4 rule set, adapted from
   * https://github.com/KRR-Oxford/Guarded-saturation/blob/bde32223ae4bc8ce084d233e7eede5ed1021adc7/src/test/java/uk/ac/ox/cs/gsat/SimpleSatTest.java#L81-L83
   */
  val arity4Rule: ImmutableList[GTGD] = ImmutableList.of(
    gtgd.parse("R(x_1, x_2), P(x_2) -> EE y_1, y_2. S(x_1, x_2, y_1, y_2), T(x_1, x_2, y_2)"),
    gtgd.parse("S(x_1, x_2, x_3, x_4) -> U(x_4)"),
    gtgd.parse("T(z_1, z_2, z_3), U(z_3) -> P(z_1)")
  )
}
