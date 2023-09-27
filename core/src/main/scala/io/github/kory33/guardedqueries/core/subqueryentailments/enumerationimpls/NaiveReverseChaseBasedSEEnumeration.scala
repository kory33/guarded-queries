package io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls

import io.github.kory33.guardedqueries.core.datalog.GuardedDatalogReverseChaseEngine
import io.github.kory33.guardedqueries.core.fol.{FunctionFreeSignature, NormalGTGD}
import io.github.kory33.guardedqueries.core.fol.NormalGTGD.SingleHeadedGTGD
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.SingleAtomMatching
import io.github.kory33.guardedqueries.core.formalinstance.{
  FormalFact,
  FormalInstance,
  QueryLikeAtom
}
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet
import io.github.kory33.guardedqueries.core.subqueryentailments.{
  LocalInstance,
  LocalInstanceTerm,
  SubqueryEntailmentEnumeration,
  SubqueryEntailmentInstance
}
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.{
  LocalName,
  RuleConstant
}
import io.github.kory33.guardedqueries.core.subsumption.localinstance.MinimallyStrongLocalInstanceSet
import io.github.kory33.guardedqueries.core.utils.datastructures.BijectiveMap
import uk.ac.ox.cs.pdq.fol.{ConjunctiveQuery, Constant, Variable}
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions.given
import io.github.kory33.guardedqueries.core.utils.{CachingFunction, FunctionSpaces}
import io.github.kory33.guardedqueries.core.utils.FunctionSpaces.{
  allFunctionsBetween,
  allInjectionsBetween,
  allPartialFunctionsBetween
}

/**
 * Enumerate all possible weakenings of `inputInstance` generated by "weakening maps", i.e. maps
 * of the form `u: LocalName => LocalInstanceTerm` such that
 *   - all `LocalName`s in `range(u)` are fix-points of `u` (i.e. `u` is a unification), and
 *   - all `LocalName`s in `localNamesToFix` are fix-points of `u`.
 *
 * TODO: this is the same function as in NaiveReverseChaseEngine.allWeakenings. Refactor.
 */
private def allWeakenings(localNamesToFix: Set[LocalName],
                          ruleConstantsInProgram: Set[RuleConstant]
)(inputInstance: LocalInstance): Iterable[LocalInstance] = {
  import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions.given

  val unifiableNames = inputInstance.activeLocalNames -- localNamesToFix
  val nonUnifiableTerms = ruleConstantsInProgram ++
    (inputInstance.activeLocalNames intersect localNamesToFix)

  for {
    unification <- unifiableNames.allPartitions
    // This is a choice of which component of `unification` to attach to which non-unifiable term
    componentAttachingMap <- FunctionSpaces
      .allPartialFunctionsBetween(unification, nonUnifiableTerms)
  } yield {
    val termMap: LocalInstanceTerm => LocalInstanceTerm = {
      case unifiableName: LocalName if !localNamesToFix.contains(unifiableName) =>
        // The partition component containing `localName`. This is guaranteed to exist because,
        // by definition of `allPartitions`, `unification` covers all unifiable names.
        val unificationComponent = unification.find(_.contains(unifiableName)).get

        // Either attach the component to a non-unifiable term, or collapse the component
        // to a single local name (namely, the minimum one in the component)
        componentAttachingMap.getOrElse(
          unificationComponent,
          unificationComponent.minBy(_.value)
        )
      case other => other
    }

    inputInstance.map(termMap)
  }
}

/**
 * An implementation of subquery entailment enumeration based on the reverse chase algorithm.
 */
class NaiveReverseChaseBasedSEEnumeration(
  reverseChaseEngine: GuardedDatalogReverseChaseEngine,
  minimalInstanceSetFactory: MinimallyStrongLocalInstanceSet.Factory
) extends SubqueryEntailmentEnumeration {
  import NaiveReverseChaseBasedSEEnumeration.SubqueryRepresentation
  import io.github.kory33.guardedqueries.core.subsumption.localinstance.MinimallyStrongLocalInstanceSet.AddResult

  def allMaximallyStrongInstancesCached(
    saturatedRuleSet: SaturatedRuleSet[NormalGTGD],
    boundVariableConnectedQuery: ConjunctiveQuery,
    maxArityOfAllPredicatesInRuleQueryPair: Int
  ): SubqueryRepresentation => Iterable[LocalInstance] = {
    def reverseChaseAndVisitLocalInstances(
      subquery: SubqueryRepresentation,
      localInstance: LocalInstance
    )(minimallyStrongInstancesSoFar: MinimallyStrongLocalInstanceSet): Unit = {

      /**
       * Reverse-chase the given instance existentially without weakening (hence the name
       * "exact").
       */
      def reverseChaseOneStepExistentiallyExact(instance: LocalInstance)
        : Iterable[LocalInstance] = {
        def withRule(existentialRule: SingleHeadedGTGD): Iterable[LocalInstance] = {
          val headAtomQuery: QueryLikeAtom[Variable, LocalInstanceTerm] =
            FormalFact.fromAtom(existentialRule.headAtom).map {
              case v: Variable => Left(v)
              case c: Constant => Right(RuleConstant(c))
              case t =>
                throw new IllegalArgumentException(s"$t is not a variable nor a constant")
            }

          // For `s` to be a valid reverse homomorphism, we demand that:
          //  1. the fact `s(rule.headAtom)` appears in `instance`
          //  2. for every existential variable `x` of `rule`, `s(x)`
          //     - is an element of the set
          //       `instance.activeLocalNames -- subquery.namesToBePreservedTowardsAncestors`
          //     - only appears in the fact `s(rule.headAtom)`
          //  3. `instance.activeLocalNames` must be a subset of terms in `s(rule.headAtom)`
          //
          // The first rule prevents pointless reverse chases. The second rule makes it possible to think that
          // the fact `s(rule.headAtom)` is a fact "introduced at `instance`" by `rule. Finally, the last rule
          // makes sure that the rest of facts in `instance` can be thought of as "inherited" by the parent instance.
          val possibleExistentialNames =
            instance.activeLocalNames -- subquery.namesToBePreservedTowardsAncestors

          val nonFrontierVariables = existentialRule.nonFrontierVariables

          SingleAtomMatching
            .allMatches(headAtomQuery, instance)
            .allHomomorphisms
            .associate { headReverseHomomorphism =>
              val materializedHead =
                headReverseHomomorphism.materializeFunctionFreeAtom(existentialRule.headAtom)
              val instanceMinusHead = instance - materializedHead
              (materializedHead, instanceMinusHead)
            }
            .filter {
              // filter by Rule 2
              case (headReverseHomomorphism, (_, instanceMinusHead)) =>
                existentialRule.getExistential
                  .map(headReverseHomomorphism.apply)
                  .forall {
                    case existentialImage: LocalName =>
                      possibleExistentialNames.contains(existentialImage) &&
                      !instanceMinusHead.activeLocalNames.contains(existentialImage)
                    case _: RuleConstant =>
                      // We must reject this reverse homomorphism as the child instance requests a rule constant,
                      // which cannot be satisfied by a null generated by such an instantiation
                      false
                  }
            }
            .filter {
              case (_, (materializedHead, instanceMinusHead)) =>
                // filter by Rule 3
                instanceMinusHead.activeLocalNames.subsetOfSupertypeSet(
                  materializedHead.appliedTerms.toSet
                )
            }
            .flatMap {
              case (headReverseHomomorphism, (_, instanceMinusHead)) =>
                // Generate all possible extensions of `headReverseHomomorphism` and add the materialized body
                // to `instanceMinusHead`.

                // The set of terms to which a reverse-homomorphism may be extended.
                // As the rule is guarded, we will never reduce active local name set by head deletion.
                // Therefore, we can only extend the homomorphism with values from one of
                //  - the set of rule constants
                //  - a set L of local names whose size is `ctx.instanceWidthUpperLimit` and
                //    that contains `instance.activeLocalNames`
                val possibleExtensionRange: Set[LocalInstanceTerm] =
                  saturatedRuleSet.constants.map(RuleConstant.apply) ++ {
                    val activeLocalNames = instance.activeLocalNames
                    activeLocalNames ++ {
                      val extraLocalNameCandidates =
                        (0 until maxArityOfAllPredicatesInRuleQueryPair)
                          .map(LocalName.apply)
                          .toSet

                      // "extra" local names not yet active in the current instance
                      // but could have been active in (an representative of an isomorphic class of) the parent instance
                      (extraLocalNameCandidates -- activeLocalNames).take(
                        maxArityOfAllPredicatesInRuleQueryPair - activeLocalNames.size
                      )
                    }
                  }

                FunctionSpaces
                  .allFunctionsBetween(nonFrontierVariables, possibleExtensionRange)
                  .map(headReverseHomomorphism.extendWithMap)
                  .map { reverseHomomorphism =>
                    instanceMinusHead ++ reverseHomomorphism.materializeFunctionFreeAtoms(
                      existentialRule.getBodyAtoms.toSet
                    )
                  }
            }
        }

        for {
          rule <- saturatedRuleSet.existentialRules
          instance <- withRule(rule.asSingleHeaded)
        } yield instance
      }

      def performDFS(current: LocalInstance): Unit = for {
        reverseFullChasedFromCurrent <- reverseChaseEngine.reverseFullChase(
          subquery.namesToBePreservedTowardsAncestors,
          saturatedRuleSet.saturatedRulesAsGuardedDatalogProgram,
          maxArityOfAllPredicatesInRuleQueryPair,
          current
        )
        if minimallyStrongInstancesSoFar.add(reverseFullChasedFromCurrent) == AddResult.Added

        weakenedReverseFullChased <- allWeakenings(
          subquery.namesToBePreservedTowardsAncestors,
          saturatedRuleSet.constants.map(RuleConstant.apply)
        )(reverseFullChasedFromCurrent)
        next <- reverseChaseOneStepExistentiallyExact(weakenedReverseFullChased)
        if minimallyStrongInstancesSoFar.add(next) == AddResult.Added
      } do performDFS(next)

      performDFS(localInstance)
    }

    // We prepare a mutual recursion between allMaximallyStrongInstances and weakestCommitPointsFor
    lazy val allMaximallyStrongInstances =
      CachingFunction { (subquery: SubqueryRepresentation) =>
        val instanceSet =
          minimalInstanceSetFactory.newSet(subquery.namesToBePreservedTowardsAncestors)

        for (weakestCommitPoint <- weakestCommitPointsFor(subquery)) do {
          reverseChaseAndVisitLocalInstances(subquery, weakestCommitPoint)(instanceSet)
        }

        instanceSet.getMinimalLocalInstances
      }

    def weakestCommitPointsFor(query: SubqueryRepresentation): Iterable[LocalInstance] = {
      for {
        commitVariables <- query.coexistentialVariables.powerset.filter(_.nonEmpty)
        unification <- commitVariables.allPartitions
        if unification.size <= maxArityOfAllPredicatesInRuleQueryPair
        unificationMap = unification
          .zipWithIndex
          .flatMap((p, i) => p.associate(_ => LocalName(i): LocalName))
          .toMap
        unifiedCommittedPart = boundVariableConnectedQuery
          .subqueryRelevantToVariables(unificationMap.keySet).get
          .strictlyInduceSubqueryByVariables(
            unificationMap.keySet ++ query.committedBoundaryVariables
          )
        extraLocalNameCandidates = {
          val unusedNames = (0 until maxArityOfAllPredicatesInRuleQueryPair)
            .map(LocalName.apply)
            .toSet -- unificationMap.values
          unusedNames.take(maxArityOfAllPredicatesInRuleQueryPair - unificationMap.size)
        }
        localNameCandidates = unificationMap.values.toSet ++ extraLocalNameCandidates
        unionOfMaximallyStrongSplitSubqueryInstances <- {
          val coexistentialComponents = boundVariableConnectedQuery.connectedComponentsOf(
            query.coexistentialVariables -- commitVariables
          )

          val boundaryVariableToLocalName =
            unificationMap ++ query.boundaryVariablesToLocalNameMap

          val splitSubqueries = coexistentialComponents.map { component =>
            val relevantSubquery = boundVariableConnectedQuery
              .subqueryRelevantToVariables(component)
              .get

            val relevantSubqueryConstants = relevantSubquery.allConstants

            val componentNeighbourhood = boundVariableConnectedQuery
              .strictNeighbourhoodOf(component)

            SubqueryRepresentation(
              component,
              query.boundaryVariablesToConstantMap,
              componentNeighbourhood
                .filterNot(query.boundaryVariablesToConstantMap.contains)
                .associate(boundaryVariableToLocalName),
              query.queryConstantEmbedding.restrictToKeys(relevantSubqueryConstants)
            )
          }

          splitSubqueries
            .productAll { subquery =>
              allMaximallyStrongInstances(subquery).flatMap { minimallyStrongInstance =>
                FunctionSpaces
                  .allFunctionsBetween(
                    minimallyStrongInstance.activeLocalNames -- subquery.namesToBePreservedTowardsAncestors,
                    localNameCandidates
                  )
                  .map(nameRemapping => minimallyStrongInstance.mapLocalNames(nameRemapping))
              }
            }
            .map(_.unionAll)
        }
      } yield {
        val committedVariableMap = query.boundaryVariableCommitMap ++ unificationMap
        val committedPart = FormalInstance(
          unifiedCommittedPart.toList.toSet
            .flatMap(_.getAtoms)
            .map(FormalFact.fromAtom)
        ).map {
          case v: Variable => committedVariableMap(v)
          case c: Constant => query.queryConstantEmbedding.asMap.getOrElse(c, RuleConstant(c))
          case t => throw new IllegalArgumentException(s"$t is not a variable nor a constant")
        }

        unionOfMaximallyStrongSplitSubqueryInstances ++ committedPart
      }
    }

    allMaximallyStrongInstances
  }

  override def apply(_extensionalSignature: FunctionFreeSignature,
                     saturatedRuleSet: SaturatedRuleSet[? <: NormalGTGD],
                     boundVariableConnectedQuery: ConjunctiveQuery
  ): Iterable[SubqueryEntailmentInstance] = {
    val maxArityOfAllPredicatesInRuleQueryPair = FunctionFreeSignature
      .encompassingRuleQuery(saturatedRuleSet.allRules, boundVariableConnectedQuery)
      .maxArity

    val allMaximallyStrongInstances =
      allMaximallyStrongInstancesCached(
        saturatedRuleSet,
        boundVariableConnectedQuery,
        maxArityOfAllPredicatesInRuleQueryPair
      )

    for {
      subquery <- NaiveReverseChaseBasedSEEnumeration.allWellFormedSubqueryRepresentations(
        boundVariableConnectedQuery,
        saturatedRuleSet.constants.map(RuleConstant.apply),
        maxArityOfAllPredicatesInRuleQueryPair
      )
      minimallyStrongInstance <- allMaximallyStrongInstances(subquery)
      // We only output instances that are minimally strong in the extensional signature
      if minimallyStrongInstance.allPredicates.subsetOf(_extensionalSignature.predicates)
    } yield subquery.toSubqueryEntailmentInstance(minimallyStrongInstance)
  }
}

object NaiveReverseChaseBasedSEEnumeration {
  case class SubqueryRepresentation(
    coexistentialVariables: Set[Variable],
    boundaryVariablesToConstantMap: Map[Variable, RuleConstant],
    boundaryVariablesToLocalNameMap: Map[Variable, LocalName],
    queryConstantEmbedding: BijectiveMap[Constant, LocalName]
  ) {
    // we must not treat these local names as potential existentials, nor unify them
    val namesToBePreservedTowardsAncestors: Set[LocalName] =
      boundaryVariablesToLocalNameMap.values.toSet ++ queryConstantEmbedding.values

    val boundaryVariableCommitMap: Map[Variable, LocalInstanceTerm] =
      boundaryVariablesToConstantMap ++ boundaryVariablesToLocalNameMap

    // Set of all variables committed (either to a local name or to a constant) in the query
    val committedBoundaryVariables: Set[Variable] = boundaryVariableCommitMap.keySet

    def toSubqueryEntailmentInstance(localInstance: LocalInstance): SubqueryEntailmentInstance =
      SubqueryEntailmentInstance(
        coexistentialVariables,
        boundaryVariablesToConstantMap,
        localInstance,
        boundaryVariablesToLocalNameMap,
        queryConstantEmbedding
      )
  }

  private def allWellFormedSubqueryRepresentations(
    conjunctiveQuery: ConjunctiveQuery,
    ruleConstants: Set[RuleConstant],
    maxArityOfAllPredicatesInRuleQueryPair: Int
  ): Iterable[SubqueryRepresentation] = {
    val queryVariables = conjunctiveQuery.allVariables
    val queryExistentialVariables = conjunctiveQuery.getBoundVariables.toSet

    for {
      boundaryVariablesToConstantMap <-
        allPartialFunctionsBetween(queryVariables, ruleConstants)
      variablesGuessedToBeRuleConstants = boundaryVariablesToConstantMap.keySet

      coexistentialVariables <- queryExistentialVariables.powerset
        .filter(_.nonEmpty)
        .filter(variableSet => conjunctiveQuery.connects(variableSet))

      relevantSubquery =
        // As coexistentialVariables is a nonempty subset of queryVariables,
        // we expect to see a non-empty optional.
        conjunctiveQuery.subqueryRelevantToVariables(coexistentialVariables).get
      nonConstantNeighbourhood =
        conjunctiveQuery
          .strictNeighbourhoodOf(coexistentialVariables) -- variablesGuessedToBeRuleConstants

      activeLocalNames <- (0 until maxArityOfAllPredicatesInRuleQueryPair * 2)
        .map(LocalName(_): LocalName).toSet
        .powerset
        .filter(_.size <= maxArityOfAllPredicatesInRuleQueryPair)
      boundaryVariablesToLocalNameMap <-
        allFunctionsBetween(nonConstantNeighbourhood, activeLocalNames)
          .filter(_.values.size == activeLocalNames.size)
      queryConstantEmbedding <- {
        val subqueryConstants = relevantSubquery.allConstants -- ruleConstants.map(_.constant)
        val nonWitnessingActiveLocalNames =
          activeLocalNames -- boundaryVariablesToLocalNameMap.values

        allInjectionsBetween(subqueryConstants, nonWitnessingActiveLocalNames)
      }
    } yield SubqueryRepresentation(
      coexistentialVariables,
      boundaryVariablesToConstantMap,
      boundaryVariablesToLocalNameMap,
      queryConstantEmbedding
    )
  }
}
