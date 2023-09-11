package io.github.kory33.guardedqueries.core.subsumption.localinstance

import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.NaturalJoinAlgorithm
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.subqueryentailments.{
  LocalInstance,
  LocalInstanceTerm
}
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.{
  LocalName,
  RuleConstant
}
import io.github.kory33.guardedqueries.core.utils.FunctionSpaces
import io.github.kory33.guardedqueries.testutils.scalacheck.GenFormalInstance
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.flatspec.AnyFlatSpec
import uk.ac.ox.cs.pdq.fol.{Constant, TypedConstant}

class LocalInstanceStrengthRelationTest extends AnyFlatSpec with ScalaCheckPropertyChecks {
  val genSmallFormalInstance: Gen[FormalInstance[Constant]] = {
    val constantsToUse = (1 to 4).map(i => TypedConstant.create(s"c_$i"): Constant).toSet
    GenFormalInstance.genSmallFormalInstanceOnConstants(constantsToUse)
  }

  val genSmallLocalInstance: Gen[LocalInstance] =
    for {
      instanceOverConstants <- genSmallFormalInstance
      constantsToConvertToLocalNames <- Gen.someOf(instanceOverConstants.activeTerms)
    } yield {
      val constantToLocalNameMap = constantsToConvertToLocalNames.zipWithIndex.map {
        case (c, i) => (c, LocalName(i))
      }.toMap

      instanceOverConstants.map(c => constantToLocalNameMap.getOrElse(c, RuleConstant(c)))
    }

  // A Gen value that generates a test triple (instance1, fixedLocalNames, instance2)
  // where instance1 is a local instance, fixedLocalNames is a subset of instance1.activeLocalNames,
  //
  // local name mapping s and an instance i3.
  val genMappedInstanceTriple: Gen[(LocalInstance, Set[LocalName], LocalInstance)] = for {
    i1 <- genSmallLocalInstance
    localNamesToFix <- Gen.someOf(i1.activeLocalNames)
    localNamesToMap = i1.activeLocalNames -- localNamesToFix
    localNameMapping <- Gen.oneOf {
      FunctionSpaces.allFunctionsBetween(localNamesToMap, (1 to 6).map(LocalName(_)).toSet)
    }
    i3 <- genSmallLocalInstance
  } yield {
    val mappedI1 = i1.mapLocalNames(ln => localNameMapping.getOrElse(ln, ln))
    (i1, localNamesToFix.toSet, mappedI1 ++ i3)
  }

  "i1 asStrongAs i2" should "be true if i2 is a superset of mapped i1" in {
    forAll(genMappedInstanceTriple, minSuccessful(300)) {
      case (i1, fixedLocalNames, i2) =>
        given NaturalJoinAlgorithm[LocalName, LocalInstanceTerm, LocalInstance] =
          FilterNestedLoopJoin()
        val localInstanceStrengthRelation = LocalInstanceStrengthRelation(fixedLocalNames)
        import localInstanceStrengthRelation.given

        assert(i1 asStrongAs i2)
    }
  }
}
