package io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls

import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import io.github.kory33.guardedqueries.core.fol.NormalGTGD
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstance
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTermFact
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentEnumeration
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentInstance
import io.github.kory33.guardedqueries.core.utils.FunctionSpaces.*
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.ListExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.MapExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.TGDExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.*
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Variable

import scala.collection.mutable
import scala.util.boundary

/**
 * An implementation of subquery entailment enumeration using a DP table.
 */
final class NaiveDPTableSEEnumeration(
  private val datalogSaturationEngine: DatalogSaturationEngine
) extends SubqueryEntailmentEnumeration {
  final private class DPTable(private val saturatedRuleSet: SaturatedRuleSet[_ <: NormalGTGD],
                              private val extensionalSignature: FunctionFreeSignature,
                              private val maxArityOfAllPredicatesUsedInRules: Int,
                              private val connectedConjunctiveQuery: ConjunctiveQuery
  ) {

    final private val table = mutable.HashMap[SubqueryEntailmentInstance, Boolean]()

    private def isYesInstance(instance: SubqueryEntailmentInstance) = {
      if (!this.table.contains(instance)) fillTableUpto(instance)
      this.table(instance)
    }

    private def chaseLocalInstance(localInstance: LocalInstance,
                                   namesToBePreservedDuringChase: Set[LocalName]
    ): Set[LocalInstance] = {
      val datalogSaturation = saturatedRuleSet.saturatedRulesAsDatalogProgram
      val shortcutChaseOneStep = (instance: LocalInstance) => {
        def foo(instance: LocalInstance): Set[LocalInstance] = {
          val localNamesUsableInChildren = {
            (0 until maxArityOfAllPredicatesUsedInRules * 2)
              .map(LocalInstanceTerm.LocalName.apply)
              .toSet -- instance.getActiveTermsIn[LocalName]
          }.toList

          // We need to chase the instance with all existential rules
          // while preserving all names in namesToBePreservedDuringChase.
          //
          // A name is preserved by a chase step if and only if
          // it appears in the substituted head of the existential rule.
          //
          // We can first find all possible homomorphisms from the body of
          // the existential rule to the instance by a join algorithm,
          // and then filter out those that do not preserve the names.
          val allChasesWithRule = (existentialRule: NormalGTGD) => {
            def foo(existentialRule: NormalGTGD): List[LocalInstance] = {
              val headAtom = existentialRule.getHeadAtoms()(0)

              // A set of existential variables in the existential rule
              val existentialVariables = existentialRule.getHead.getBoundVariables.toSet

              // An assignment existential variables into "fresh" local names not used in the parent
              val headVariableHomomorphism =
                existentialVariables
                  .zipWithIndex
                  .map { (variable, index) => (variable, localNamesUsableInChildren(index)) }
                  .toMap

              val bodyJoinResult = FilterNestedLoopJoin[LocalInstanceTerm].join(
                existentialRule.bodyAsCQ,
                instance
              )
              val extendedJoinResult =
                bodyJoinResult.extendWithConstantHomomorphism(headVariableHomomorphism)

              val allSubstitutedHeadAtoms =
                extendedJoinResult.materializeFunctionFreeAtom(headAtom)

              allSubstitutedHeadAtoms.flatMap(
                (substitutedHead: FormalFact[LocalInstanceTerm]) => {
                  def foo(substitutedHead: FormalFact[LocalInstanceTerm])
                    : IterableOnce[LocalInstance] = {
                    // The instance containing only the head atom produced by the existential rule.
                    // This should be a singleton instance because the existential rule is normal.
                    val headInstance = FormalInstance.of(substitutedHead)
                    val localNamesInHead =
                      headInstance.getActiveTermsIn[LocalName]

                    // if names are not preserved, we reject this homomorphism
                    if (!namesToBePreservedDuringChase.subsetOf(localNamesInHead))
                      return Set.empty

                    // the set of facts in the parent instance that are
                    // "guarded" by the head of the existential rule
                    val inheritedFactsInstance = instance.restrictToAlphabetsWith(term =>
                      term.isConstantOrSatisfies(localNamesInHead.contains)
                    )

                    // The child instance, which is the saturation of the union of
                    // the set of inherited facts and the head instance.
                    val childInstance =
                      datalogSaturationEngine.saturateUnionOfSaturatedAndUnsaturatedInstance(
                        datalogSaturation,
                        // because the parent is saturated, a restriction of it to the alphabet
                        // occurring in the child is also saturated.
                        inheritedFactsInstance,
                        headInstance
                      )

                    // we only need to keep chasing with extensional signature
                    Set(childInstance.restrictToSignature(extensionalSignature))
                  }
                  foo(substitutedHead)
                }
              )
            }
            foo(existentialRule)
          }

          val children =
            saturatedRuleSet.existentialRules.flatMap(allChasesWithRule.apply)

          children
        }

        foo(instance)
      }

      // we keep chasing until we reach a fixpoint
      Set(datalogSaturationEngine.saturateInstance(
        datalogSaturation,
        localInstance
      )).generateFromElementsUntilFixpoint(shortcutChaseOneStep.apply.apply)
    }

    /**
     * Fill the DP table up to the given instance.
     */
    def fillTableUpto(instance: SubqueryEntailmentInstance): Unit = boundary { returnMethod ?=>
      // The subquery for which we are trying to decide the entailment problem.
      // If the instance is well-formed, the variable set is non-empty and connected,
      // so the set of relevant atoms must be non-empty. Therefore the .get() call succeeds.
      // noinspection OptionalGetWithoutIsPresent
      val relevantSubquery = connectedConjunctiveQuery.subqueryRelevantToVariables(
        instance.coexistentialVariables
      ).get

      val instancesWithGuessedVariablesPreserved = chaseLocalInstance(
        instance.localInstance,
        // we need to preserve all local names in the range of localWitnessGuess and queryConstantEmbedding
        // because they are treated as special symbols corresponding to variables and query constants
        // occurring in the subquery.
        instance.localWitnessGuess.values.toSet ++
          instance.queryConstantEmbedding.values
      )

      for (chasedInstance <- instancesWithGuessedVariablesPreserved) {
        val localWitnessGuessExtensions = allPartialFunctionsBetween(
          instance.coexistentialVariables,
          chasedInstance.getActiveTermsIn[LocalName]
        )

        for (localWitnessGuessExtension <- localWitnessGuessExtensions) {
          boundary: continueInnerLoop ?=>
            if (localWitnessGuessExtension.isEmpty) {
              // we do not allow "empty split"; whenever we split (i.e. make some progress
              // in the chase automaton), we must pick a nonempty set of coexistential variables
              // to map to local names in the chased instance.
              boundary.break()(using continueInnerLoop)
            }

            val newlyCoveredVariables = localWitnessGuessExtension.keySet
            val extendedLocalWitnessGuess =
              instance.localWitnessGuess.toMap ++ localWitnessGuessExtension

            val newlyCoveredAtomsOccurInChasedInstance = {
              val extendedGuess =
                extendedLocalWitnessGuess ++ instance.ruleConstantWitnessGuessAsMapToInstanceTerms

              val newlyCoveredAtoms = relevantSubquery.getAtoms.filter((atom: Atom) => {
                val atomVariables = atom.getVariables.toSet
                val allVariablesAreCovered = atomVariables.subsetOf(extendedGuess.keySet)

                // we no longer care about the part of the query
                // which entirely lies in the neighborhood of coexistential variables
                // of the instance
                val someVariableIsNewlyCovered =
                  atomVariables.intersects(newlyCoveredVariables)

                allVariablesAreCovered && someVariableIsNewlyCovered
              })

              newlyCoveredAtoms.map((atom: Atom) =>
                LocalInstanceTermFact.fromAtomWithVariableMap(atom, extendedGuess.apply)
              ).forall(chasedInstance.containsFact)
            }

            if (!newlyCoveredAtomsOccurInChasedInstance)
              boundary.break()(using continueInnerLoop)

            val allSplitInstancesAreYesInstances = {
              val splitCoexistentialVariables = relevantSubquery.connectedComponentsOf(
                instance.coexistentialVariables -- newlyCoveredVariables
              )

              splitCoexistentialVariables.forall(splitCoexistentialVariablesComponent => {
                val newNeighbourhood = relevantSubquery
                  .strictNeighbourhoodOf(
                    splitCoexistentialVariablesComponent
                  ) -- instance.ruleConstantWitnessGuess.keySet

                // For the same reason as .get() call in the beginning of the method,
                // this .get() call succeeds.
                // noinspection OptionalGetWithoutIsPresent
                val newRelevantSubquery =
                  relevantSubquery.subqueryRelevantToVariables(
                    splitCoexistentialVariablesComponent
                  ).get

                val inducedInstance = SubqueryEntailmentInstance(
                  instance.ruleConstantWitnessGuess,
                  splitCoexistentialVariablesComponent,
                  chasedInstance,
                  extendedLocalWitnessGuess.restrictToKeys(newNeighbourhood),
                  instance.queryConstantEmbedding.restrictToKeys(
                    newRelevantSubquery.allConstants
                  )
                )
                isYesInstance(inducedInstance)

              })
            }

            if (allSplitInstancesAreYesInstances) {
              this.table.put(instance, true)
              boundary.break()(using returnMethod)
            }
        }
      }

      // all instances chased from the original instance fail to fulfill the subquery
      // strongly induced by instance.coexistentialVariables(), so we mark the original instance false.
      this.table.put(instance, false)
    }

    def getKnownYesInstances: IterableOnce[SubqueryEntailmentInstance] =
      this.table.filter(_._2).keys
  }

  def apply(extensionalSignature: FunctionFreeSignature,
            saturatedRuleSet: SaturatedRuleSet[_ <: NormalGTGD],
            connectedConjunctiveQuery: ConjunctiveQuery
  ): IterableOnce[SubqueryEntailmentInstance] = {
    val ruleConstants = saturatedRuleSet.constants
    val maxArityOfAllPredicatesUsedInRules = FunctionFreeSignature.encompassingRuleQuery(
      saturatedRuleSet.allRules,
      connectedConjunctiveQuery
    ).maxArity
    val dpTable = new DPTable(
      saturatedRuleSet,
      extensionalSignature,
      maxArityOfAllPredicatesUsedInRules,
      connectedConjunctiveQuery
    )

    // NOTE:
    //   This algorithm is massively inefficient as-is.
    //   Here are a few optimization points that we could further explore:
    //    - Problem 1:
    //        We actually only need to consider local instances that are
    //        (1) saturated by the input ruleset and (2) guarded as an instance.
    //        The implementation in this class brute-forces all possible local instances,
    //        so we are simply exploring a much larger search space than necessary.
    //    - Problem 2:
    //        Once we mark a problem instance as a true instance, we no longer have to fill the table
    //        for "larger" local instances due to the subsumption.
    //        It is easy to see that, for problems instances `sqei1` and `sqei2`, if
    //         - `sqei2.coexistentialVariables` equals `sqei1.coexistentialVariables`
    //         - there exists a function θ (a "matching" of local names) that sends active local names
    //           in `sqei1.localInstance` to active terms (so either local names or rule constants)
    //           in `sqei2.localInstance`, such that
    //           - `θ(sqei1.localInstance)` is a subinstance of `sqei2.localInstance`
    //           - `θ . (sqei1.ruleConstantWitnessGuess union sqei1.localWitnessGuess)`
    //             equals `sqei2.ruleConstantWitnessGuess union sqei2.localWitnessGuess` as a map
    //           - `θ . (sqei1.queryConstantEmbedding)` equals `sqei2.queryConstantEmbedding` as a map
    //        then `sqei1` being a true instance implies `sqei2` being a true instance.
    //    - Problem 3:
    //        During the chase phase, we can always "normalize" local instances so that active values are
    //        always within the range of {0,1,...,maxArity-1}. Moreover, we can rearrange
    //        the local names in the local instance so that the guard atom (which is chosen according
    //        to a canonical order on the predicate names) has its local-name parameters in the increasing
    //        order. This way, we can identify a number of local instances that have the "same shape",
    //        avoiding the need to explore all possible local instances.
    //    - Problem 4:
    //        During the chase phase, if we happen to mark the root problem instance as a true instance,
    //        we can mark all "intermediate" local instances between the root and the successful branching point
    //        as true, too. On the other hand, if we happen to mark the root problem instance as a false instance,
    //        we can mark all local instances below the root as false, too.
    //        The implementation in this class completely ignores this aspect of the tree-structure of the chase.
    //
    //   Out of these four problems,
    //    - Problem 3 has been addressed by NormalizingDPTableSEEnumeration, and
    //    - Problem 4 has been further addressed by DFSNormalizingDPTableSEEnumeration.
    //
    //   The most crucial optimization point is Problem 2, which greatly affects how much
    //   exponential blowup we have to deal with. The challenge is to implement the following:
    //    - dynamically pruning the search space,
    //      i.e. *not even generating* problem instances that are already known to be
    //       - false, which we will not add to the DP table anyway
    //       - true, which is subsumed by some other instance already marked as true
    //    - keeping track of only "maximally subsuming true instances" and "minimally subsuming false instances"
    //    - efficiently matching a problem instance to other subsuming instances using indexing techniques
    NaiveDPTableSEEnumeration.allWellFormedSubqueryEntailmentInstancesFor(
      extensionalSignature,
      ruleConstants,
      connectedConjunctiveQuery
    ).foreach(dpTable.fillTableUpto)
    dpTable.getKnownYesInstances
  }
  override def toString: String =
    "NaiveDPTableSEEnumeration{" + "datalogSaturationEngine=" + datalogSaturationEngine + '}'
}

object NaiveDPTableSEEnumeration {
  private def allLocalInstances(extensionalSignature: FunctionFreeSignature,
                                ruleConstants: Set[Constant]
  ) = {
    val maxArityOfExtensionalSignature = extensionalSignature.maxArity
    val ruleConstantsAsLocalTerms = ruleConstants.map(LocalInstanceTerm.RuleConstant.apply)

    // We need to consider sufficiently large collection of set of active local names.
    // As it is sufficient to check subquery entailments for all guarded instance
    // over the extensional signature, and the extensional signature has
    // maxArityOfExtensionalSignature as the maximal arity, we only need to
    // consider a powerset of {0, ..., 2 * maxArityOfExtensionalSignature - 1}
    // with size at most maxArityOfExtensionalSignature.
    val allActiveLocalNames = (0 until maxArityOfExtensionalSignature * 2)
      .toSet
      .powerset
      .filter(_.size <= maxArityOfExtensionalSignature)

    allActiveLocalNames.flatMap((localNameSet: Set[Int]) => {
      def foo(localNameSet: Set[Int]) = {
        val localNames = localNameSet.map(LocalInstanceTerm.LocalName.apply)

        val allLocalInstanceTerms = localNames ++ ruleConstantsAsLocalTerms
        val predicates = extensionalSignature.predicates
        val allLocalInstancesOverThePredicate = (predicate: Predicate) => {
          val predicateParameterIndices = (0 until predicate.getArity).toSet

          val allFormalFactsOverThePredicate =
            allTotalFunctionsBetween(predicateParameterIndices, allLocalInstanceTerms).map(
              parameterMap => {
                val parameterList = (0 until predicate.getArity).map(parameterMap.apply)
                FormalFact[LocalInstanceTerm](predicate, parameterList.toList)
              }
            ).toSet

          allFormalFactsOverThePredicate.powerset.map(FormalInstance.apply)
        }

        val allInstancesOverLocalNameSet = predicates.toList
          .productMappedIterablesToLists(allLocalInstancesOverThePredicate)
          .map(FormalInstance.unionAll)

        allInstancesOverLocalNameSet.filter(
          _.getActiveTermsIn[LocalName].size == localNameSet.size
        )
      }

      foo(localNameSet)
    })
  }

  private def allWellFormedSubqueryEntailmentInstancesFor(
    extensionalSignature: FunctionFreeSignature,
    ruleConstants: Set[Constant],
    conjunctiveQuery: ConjunctiveQuery
  ) = {
    val queryVariables = conjunctiveQuery.allVariables
    val queryExistentialVariables = conjunctiveQuery.getBoundVariables.toSet

    allPartialFunctionsBetween(queryVariables, ruleConstants).flatMap(
      (ruleConstantWitnessGuess: Map[Variable, Constant]) => {
        val allCoexistentialVariableSets = queryExistentialVariables.powerset
          .filter(_.nonEmpty)
          .filter(!_.intersects(ruleConstantWitnessGuess.keySet))
          .filter(variableSet => conjunctiveQuery.connects(variableSet))

        allCoexistentialVariableSets.flatMap((coexistentialVariables: Set[Variable]) =>
          allLocalInstances(extensionalSignature, ruleConstants).flatMap(
            (localInstance: LocalInstance) => {
              // As coexistentialVariables is a nonempty subset of queryVariables,
              // we expect to see a non-empty optional.
              // noinspection OptionalGetWithoutIsPresent
              val relevantSubquery = conjunctiveQuery.subqueryRelevantToVariables(
                coexistentialVariables
              ).get
              val nonConstantNeighbourhood =
                conjunctiveQuery.strictNeighbourhoodOf(
                  coexistentialVariables
                ) -- ruleConstantWitnessGuess.keySet

              val allLocalWitnessGuesses = allTotalFunctionsBetween(
                nonConstantNeighbourhood,
                localInstance.getActiveTermsIn[LocalName]
              )

              allLocalWitnessGuesses.flatMap(localWitnessGuess => {
                val subqueryConstants = relevantSubquery.allConstants -- ruleConstants

                val nonWitnessingActiveLocalNames =
                  localInstance.getActiveTermsIn[LocalName] -- localWitnessGuess.values

                val allQueryConstantEmbeddings = allInjectiveTotalFunctionsBetween(
                  subqueryConstants,
                  nonWitnessingActiveLocalNames
                )

                allQueryConstantEmbeddings.map(queryConstantEmbedding =>
                  SubqueryEntailmentInstance(
                    ruleConstantWitnessGuess,
                    coexistentialVariables,
                    localInstance,
                    localWitnessGuess,
                    queryConstantEmbedding
                  )
                )
              })
            }
          )
        )
      }
    )
  }
}
