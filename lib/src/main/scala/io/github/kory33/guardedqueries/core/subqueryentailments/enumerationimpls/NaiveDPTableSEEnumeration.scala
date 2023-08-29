package io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls

import scala.jdk.CollectionConverters.*

import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine
import io.github.kory33.guardedqueries.core.fol.{FunctionFreeSignature, NormalGTGD}
import io.github.kory33.guardedqueries.core.formalinstance.{FormalFact, FormalInstance}
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName
import io.github.kory33.guardedqueries.core.subqueryentailments.{
  LocalInstanceTerm,
  SubqueryEntailmentEnumeration,
  SubqueryEntailmentInstance
}
import io.github.kory33.guardedqueries.core.utils.extensions.*
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Variable

import java.util
import java.util.stream.IntStream
import java.util.stream.Stream
import io.github.kory33.guardedqueries.core.utils.MappingStreams.*
import uk.ac.ox.cs.pdq.fol.Atom
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTermFact

/**
 * An implementation of subquery entailment enumeration using a DP table.
 */
object NaiveDPTableSEEnumeration {
  private def allLocalInstances(extensionalSignature: FunctionFreeSignature,
                                ruleConstants: Set[Constant]
  ) = {
    val maxArityOfExtensionalSignature = extensionalSignature.maxArity
    val ruleConstantsAsLocalTerms = ruleConstants.map(LocalInstanceTerm.RuleConstant(_))

    // We need to consider sufficiently large collection of set of active local names.
    // As it is sufficient to check subquery entailments for all guarded instance
    // over the extensional signature, and the extensional signature has
    // maxArityOfExtensionalSignature as the maximal arity, we only need to
    // consider a powerset of {0, ..., 2 * maxArityOfExtensionalSignature - 1}
    // with size at most maxArityOfExtensionalSignature.
    val allActiveLocalNames = SetLikeExtensions.powerset(
      (0 until maxArityOfExtensionalSignature * 2).toSet
    ).filter(_.size <= maxArityOfExtensionalSignature)

    allActiveLocalNames.flatMap((localNameSet: Set[Int]) => {
      def foo(localNameSet: Set[Int]) = {
        val localNames = localNameSet.map(LocalInstanceTerm.LocalName(_))

        val allLocalInstanceTerms = localNames ++ ruleConstantsAsLocalTerms
        val predicates = extensionalSignature.predicates.toSet
        val allLocalInstancesOverThePredicate = (predicate: Predicate) => {
          val predicateParameterIndices = (0 until predicate.getArity).toSet

          val allFormalFactsOverThePredicate =
            allTotalFunctionsBetween(predicateParameterIndices, allLocalInstanceTerms).map(
              parameterMap => {
                val parameterList = (0 until predicate.getArity).map(parameterMap(_))
                FormalFact[LocalInstanceTerm](predicate, parameterList.toList)
              }
            ).toSet

          SetLikeExtensions.powerset(allFormalFactsOverThePredicate).map(FormalInstance(_))
        }

        val allInstancesOverLocalNameSet =
          ListExtensions.productMappedIterablesToLists(
            predicates.toList,
            allLocalInstancesOverThePredicate
          ).map(FormalInstance.unionAll)

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
    val queryVariables = ConjunctiveQueryExtensions.variablesIn(conjunctiveQuery)
    val queryExistentialVariables = conjunctiveQuery.getBoundVariables.toSet

    allPartialFunctionsBetween(queryVariables, ruleConstants).flatMap(
      (ruleConstantWitnessGuess: Map[Variable, Constant]) => {
        val allCoexistentialVariableSets =
          SetLikeExtensions.powerset(queryExistentialVariables)
            .filter(_.nonEmpty)
            .filter(_.exists(ruleConstantWitnessGuess.keySet.contains))
            .filter((variableSet: Set[Variable]) =>
              ConjunctiveQueryExtensions.isConnected(conjunctiveQuery, variableSet.toSet)
            )

        allCoexistentialVariableSets.flatMap((coexistentialVariables: Set[Variable]) =>
          allLocalInstances(extensionalSignature, ruleConstants).flatMap(
            (localInstance: FormalInstance[LocalInstanceTerm]) => {
              // As coexistentialVariables is a nonempty subset of queryVariables,
              // we expect to see a non-empty optional.
              // noinspection OptionalGetWithoutIsPresent
              val relevantSubquery = ConjunctiveQueryExtensions.subqueryRelevantToVariables(
                coexistentialVariables.toSet
              )(conjunctiveQuery).get
              val nonConstantNeighbourhood =
                ConjunctiveQueryExtensions.neighbourhoodVariables(
                  conjunctiveQuery,
                  coexistentialVariables.toSet
                ) -- ruleConstantWitnessGuess.keySet

              val allLocalWitnessGuesses = allTotalFunctionsBetween(
                nonConstantNeighbourhood,
                localInstance.getActiveTermsIn[LocalName]
              )

              allLocalWitnessGuesses.flatMap(localWitnessGuess => {
                val subqueryConstants =
                  ConjunctiveQueryExtensions.constantsIn(relevantSubquery) -- ruleConstants

                val nonWitnessingActiveLocalNames =
                  localInstance.getActiveTermsIn[LocalName] -- localWitnessGuess.values

                val allQueryConstantEmbeddings = allInjectiveTotalFunctionsBetween(
                  subqueryConstants,
                  nonWitnessingActiveLocalNames
                )

                allQueryConstantEmbeddings.map(queryConstantEmbedding =>
                  new SubqueryEntailmentInstance(
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

final class NaiveDPTableSEEnumeration(
  private val datalogSaturationEngine: DatalogSaturationEngine
) extends SubqueryEntailmentEnumeration {
  final private class DPTable(private val saturatedRuleSet: SaturatedRuleSet[_ <: NormalGTGD],
                              private val extensionalSignature: FunctionFreeSignature,
                              private val maxArityOfAllPredicatesUsedInRules: Int,
                              private val connectedConjunctiveQuery: ConjunctiveQuery
  ) {

    final private val table = new util.HashMap[SubqueryEntailmentInstance, Boolean]

    private def isYesInstance(instance: SubqueryEntailmentInstance) = {
      if (!this.table.containsKey(instance)) fillTableUpto(instance)
      this.table.get(instance)
    }

    private def chaseLocalInstance(localInstance: FormalInstance[LocalInstanceTerm],
                                   namesToBePreservedDuringChase: Set[LocalName]
    ) = {
      val datalogSaturation = saturatedRuleSet.saturatedRulesAsDatalogProgram
      val shortcutChaseOneStep = (instance: FormalInstance[LocalInstanceTerm]) => {
        def foo(instance: FormalInstance[LocalInstanceTerm]) = {
          val localNamesUsableInChildren = {
            (0 until maxArityOfAllPredicatesUsedInRules * 2)
              .map(LocalInstanceTerm.LocalName(_))
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
            def foo(existentialRule: NormalGTGD) = {
              val headAtom = existentialRule.getHeadAtoms()(0)

              // A set of existential variables in the existential rule
              val existentialVariables = existentialRule.getHead.getBoundVariables.toSet

              // An assignment existential variables into "fresh" local names not used in the parent
              val headVariableHomomorphism =
                existentialVariables
                  .zipWithIndex
                  .map { (variable, index) => (variable, localNamesUsableInChildren(index)) }
                  .toMap

              val bodyJoinResult = new FilterNestedLoopJoin[LocalInstanceTerm](
                LocalInstanceTerm.RuleConstant(_)
              ).join(
                TGDExtensions.bodyAsCQ(existentialRule),
                instance
              )
              val extendedJoinResult =
                bodyJoinResult.extendWithConstantHomomorphism(headVariableHomomorphism)

              val allSubstitutedHeadAtoms = extendedJoinResult.materializeFunctionFreeAtom(
                headAtom,
                LocalInstanceTerm.RuleConstant(_)
              )

              allSubstitutedHeadAtoms.flatMap(
                (substitutedHead: FormalFact[LocalInstanceTerm]) => {
                  def foo(substitutedHead: FormalFact[LocalInstanceTerm])
                    : IterableOnce[FormalInstance[LocalInstanceTerm]] = {
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
                    val inheritedFactsInstance = instance.restrictToAlphabetsWith((term) =>
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
                        headInstance,
                        LocalInstanceTerm.RuleConstant(_)
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
            saturatedRuleSet.existentialRules.flatMap(allChasesWithRule(_))

          children.toSet
        }

        foo(instance)
      }

      // we keep chasing until we reach a fixpoint
      SetLikeExtensions.generateFromElementsUntilFixpoint(
        Set(datalogSaturationEngine.saturateInstance(
          datalogSaturation,
          localInstance,
          LocalInstanceTerm.RuleConstant(_)
        )),
        shortcutChaseOneStep.apply(_)
      )
    }

    /**
     * Fill the DP table up to the given instance.
     */
    def fillTableUpto(instance: SubqueryEntailmentInstance): Unit = {
      // The subquery for which we are trying to decide the entailment problem.
      // If the instance is well-formed, the variable set is non-empty and connected,
      // so the set of relevant atoms must be non-empty. Therefore the .get() call succeeds.
      // noinspection OptionalGetWithoutIsPresent
      val relevantSubquery = ConjunctiveQueryExtensions.subqueryRelevantToVariables(
        instance.coexistentialVariables.toSet
      )(connectedConjunctiveQuery).get

      val instancesWithGuessedVariablesPreserved = chaseLocalInstance(
        instance.localInstance,
        // we need to preserve all local names in the range of localWitnessGuess and queryConstantEmbedding
        // because they are treated as special symbols corresponding to variables and query constants
        // occurring in the subquery.
        instance.localWitnessGuess.values.toSet ++
          instance.queryConstantEmbedding.values.asScala
      )

      for (chasedInstance <- instancesWithGuessedVariablesPreserved) {
        val localWitnessGuessExtensions = allPartialFunctionsBetween(
          instance.coexistentialVariables,
          chasedInstance.getActiveTermsIn[LocalName]
        )

        for (localWitnessGuessExtension <- localWitnessGuessExtensions) {
          import scala.util.boundary

          boundary:
            if (localWitnessGuessExtension.isEmpty) {
              // we do not allow "empty split"; whenever we split (i.e. make some progress
              // in the chase automaton), we must pick a nonempty set of coexistential variables
              // to map to local names in the chased instance.
              boundary.break()
            }

            val newlyCoveredVariables = localWitnessGuessExtension.keySet
            val extendedLocalWitnessGuess =
              instance.localWitnessGuess.toMap ++ localWitnessGuessExtension

            val newlyCoveredAtomsOccurInChasedInstance = {
              val extendedGuess =
                extendedLocalWitnessGuess ++ instance.ruleConstantWitnessGuessAsMapToInstanceTerms

              val newlyCoveredAtoms =
                util.Arrays.stream(relevantSubquery.getAtoms).filter((atom: Atom) => {
                  val atomVariables = atom.getVariables.toSet
                  val allVariablesAreCovered = atomVariables.subsetOf(extendedGuess.keySet)

                  // we no longer care about the part of the query
                  // which entirely lies in the neighborhood of coexistential variables
                  // of the instance
                  val someVariableIsNewlyCovered =
                    atomVariables.exists(newlyCoveredVariables.contains)

                  allVariablesAreCovered && someVariableIsNewlyCovered
                })

              newlyCoveredAtoms.map((atom: Atom) =>
                LocalInstanceTermFact.fromAtomWithVariableMap(atom, extendedGuess(_))
              ).allMatch(chasedInstance.containsFact)
            }

            if (!newlyCoveredAtomsOccurInChasedInstance)
              boundary.break()

            val allSplitInstancesAreYesInstances = {
              val splitCoexistentialVariables =
                ConjunctiveQueryExtensions.connectedComponents(
                  relevantSubquery,
                  instance.coexistentialVariables.toSet -- newlyCoveredVariables
                )

              splitCoexistentialVariables.forall(splitCoexistentialVariablesComponent => {
                val newNeighbourhood =
                  ConjunctiveQueryExtensions.neighbourhoodVariables(
                    relevantSubquery,
                    splitCoexistentialVariablesComponent
                  ) -- instance.ruleConstantWitnessGuess.keySet

                // For the same reason as .get() call in the beginning of the method,
                // this .get() call succeeds.
                // noinspection OptionalGetWithoutIsPresent
                val newRelevantSubquery =
                  ConjunctiveQueryExtensions.subqueryRelevantToVariables(
                    splitCoexistentialVariablesComponent
                  )(relevantSubquery).get

                val inducedInstance = new SubqueryEntailmentInstance(
                  instance.ruleConstantWitnessGuess,
                  splitCoexistentialVariablesComponent,
                  chasedInstance,
                  MapExtensions.restrictToKeys(extendedLocalWitnessGuess, newNeighbourhood),
                  MapExtensions.restrictToKeys(
                    instance.queryConstantEmbedding,
                    ConjunctiveQueryExtensions.constantsIn(newRelevantSubquery)
                  )
                )
                isYesInstance(inducedInstance)

              })
            }

            if (allSplitInstancesAreYesInstances) {
              this.table.put(instance, true)
              return
            }
        }
      }

      // all instances chased from the original instance fail to fulfill the subquery
      // strongly induced by instance.coexistentialVariables(), so we mark the original instance false.
      this.table.put(instance, false)
    }

    def getKnownYesInstances: IterableOnce[SubqueryEntailmentInstance] =
      this.table.entrySet.asScala.filter(_.getValue).map(_.getKey)
  }

  def apply(extensionalSignature: FunctionFreeSignature,
            saturatedRuleSet: SaturatedRuleSet[_ <: NormalGTGD],
            connectedConjunctiveQuery: ConjunctiveQuery
  ): IterableOnce[SubqueryEntailmentInstance] = {
    val ruleConstants = saturatedRuleSet.constants
    val maxArityOfAllPredicatesUsedInRules = FunctionFreeSignature.encompassingRuleQuery(
      saturatedRuleSet.allRules.toSet,
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
