package io.github.kory33.guardedqueries.core.subqueryentailments.enumerationimpls

import io.github.kory33.guardedqueries.core.datalog.DatalogSaturationEngine
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import io.github.kory33.guardedqueries.core.fol.NormalGTGD
import io.github.kory33.guardedqueries.core.formalinstance.FormalFact
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.formalinstance.joins.HomomorphicMapping
import io.github.kory33.guardedqueries.core.formalinstance.joins.naturaljoinalgorithms.FilterNestedLoopJoin
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTermFact
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentEnumeration
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentInstance
import io.github.kory33.guardedqueries.core.utils.MappingStreams.*
import io.github.kory33.guardedqueries.core.utils.extensions.*
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Constant
import uk.ac.ox.cs.pdq.fol.Predicate
import uk.ac.ox.cs.pdq.fol.Variable

import java.util
import scala.jdk.CollectionConverters.*
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.connects
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.strictNeighbourhoodOf
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.subqueryRelevantToVariables
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.connectedComponentsOf

/**
 * An implementation of subquery entailment enumeration using a DP table plus a simple
 * normalization.
 */
object NormalizingDPTableSEEnumeration {

  /**
   * Checks whether the given set of local names is of a form {0, ..., n - 1} for some n.
   */
  private def isZeroStartingContiguousLocalNameSet(localNames: Set[LocalName]) = {
    var firstElementAfterZeroNotContainedInSet = 0
    while (localNames.contains(new LocalName(firstElementAfterZeroNotContainedInSet)))
      firstElementAfterZeroNotContainedInSet += 1
    firstElementAfterZeroNotContainedInSet == localNames.size
  }

  private def allNormalizedLocalInstances(extensionalSignature: FunctionFreeSignature,
                                          ruleConstants: Set[Constant]
  ) = {
    val maxArityOfExtensionalSignature = extensionalSignature.maxArity
    val ruleConstantsAsLocalTerms = ruleConstants.map(LocalInstanceTerm.RuleConstant(_))

    // We need to consider sufficiently large collection of set of active local names.
    // As it is sufficient to check subquery entailments for all guarded instance
    // over the extensional signature, and the extensional signature has
    // maxArityOfExtensionalSignature as the maximal arity, we only need to
    // consider a powerset of {0, ..., maxArityOfExtensionalSignature - 1}
    // (NORMALIZATION:
    //  By remapping all local names in the range
    //  [maxArityOfExtensionalSignature, maxArityOfExtensionalSignature * 2)
    //  to the range [0, maxArityOfExtensionalSignature), since the size of
    //  active local name set of local instances necessary to check
    //  is at most maxArityOfExtensionalSignature.
    //  Moreover, by symmetry of instance we can demand that the set of active
    //  names to be contiguous and starting from 0, i.e. {0, ..., n} for some n < maxArityOfExtensionalSignature.
    // )
    val localNames = (0 until maxArityOfExtensionalSignature)
      .map(LocalInstanceTerm.LocalName(_))
      .toSet

    val allLocalInstanceTerms = localNames ++ ruleConstantsAsLocalTerms
    val predicates = extensionalSignature.predicates.toSet

    val allLocalInstancesOverThePredicate = (predicate: Predicate) => {
      val predicateParameterIndices = (0 until predicate.getArity).toSet

      val allFormalFactsOverThePredicate = allTotalFunctionsBetween(
        predicateParameterIndices,
        allLocalInstanceTerms
      ).map(parameterMap => {
        FormalFact(
          predicate,
          (0 until predicate.getArity).map(parameterMap(_)).toList
        )
      })

      SetLikeExtensions
        .powerset(allFormalFactsOverThePredicate.toSet)
        .map(FormalInstance(_))
    }

    val allInstancesOverLocalNameSet = ListExtensions.productMappedIterablesToLists(
      predicates.toList,
      allLocalInstancesOverThePredicate
    ).map(FormalInstance.unionAll)

    allInstancesOverLocalNameSet.filter(instance =>
      isZeroStartingContiguousLocalNameSet(instance.getActiveTermsIn[LocalName])
    )
  }

  private def allWellFormedNormalizedSubqueryEntailmentInstancesFor(
    extensionalSignature: FunctionFreeSignature,
    ruleConstants: Set[Constant],
    conjunctiveQuery: ConjunctiveQuery
  ) = {
    val queryVariables = ConjunctiveQueryExtensions.allVariables(conjunctiveQuery).toSet
    val queryExistentialVariables = conjunctiveQuery.getBoundVariables.toSet

    allPartialFunctionsBetween(queryVariables, ruleConstants).flatMap(
      (ruleConstantWitnessGuess: Map[Variable, Constant]) => {

        val allCoexistentialVariableSets =
          SetLikeExtensions
            .powerset(queryExistentialVariables)
            .filter(_.nonEmpty)
            .filter(!_.exists(ruleConstantWitnessGuess.keySet.contains))
            .filter((variableSet: Set[Variable]) =>
              conjunctiveQuery.connects(variableSet.toSet)
            )

        allCoexistentialVariableSets.flatMap((coexistentialVariables: Set[Variable]) =>
          allNormalizedLocalInstances(extensionalSignature, ruleConstants).flatMap(
            (localInstance: FormalInstance[LocalInstanceTerm]) => {
              // As coexistentialVariables is a nonempty subset of queryVariables,
              // we expect to see a non-empty optional.
              // noinspection OptionalGetWithoutIsPresent
              val relevantSubquery = conjunctiveQuery
                .subqueryRelevantToVariables(coexistentialVariables.toSet)
                .get

              val nonConstantNeighbourhood =
                conjunctiveQuery.strictNeighbourhoodOf(
                  coexistentialVariables.toSet
                ) -- ruleConstantWitnessGuess.keySet

              val allLocalWitnessGuesses = allTotalFunctionsBetween(
                nonConstantNeighbourhood,
                localInstance
                  .getActiveTermsIn[LocalName]
                  .filter(_.value < nonConstantNeighbourhood.size)
                  .toSet
              )

              allLocalWitnessGuesses.flatMap(localWitnessGuess => {
                val subqueryConstants =
                  ConjunctiveQueryExtensions.allConstants(relevantSubquery) -- ruleConstants

                val nonWitnessingActiveLocalNames =
                  localInstance.getActiveTermsIn[LocalName] --
                    localWitnessGuess.values

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

final class NormalizingDPTableSEEnumeration(
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
          // We need to chase the instance with all existential rules
          // while preserving all names in namesToBePreservedDuringChase.
          //
          // A name is preserved by a chase step if and only if
          // it appears in the substituted head of the existential rule.
          //
          // We can first find all possible homomorphisms from the body of
          // the existential rule to the instance by a join algorithm,
          // and then filter out those that do not preserve the names.
          //
          // NORMALIZATION: unlike in NaiveDPTableSEEnumeration,
          // when we apply an existential rule, we "reuse" local names below
          // maxArityOfAllPredicatesUsedInRules (since we don't care
          // about the identity of local names at all, we can ignore the
          // "direct equivalence" semantics for implicitly-equality-coded
          // tree codes).
          val allChasesWithRule = (existentialRule: NormalGTGD) => {
            def foo(existentialRule: NormalGTGD) = {
              val headAtom = existentialRule.getHeadAtoms()(0)

              // A set of existential variables in the existential rule
              val existentialVariables = existentialRule.getHead.getBoundVariables.toSet

              val bodyJoinResult = FilterNestedLoopJoin[LocalInstanceTerm](
                LocalInstanceTerm.RuleConstant(_)
              ).join(
                TGDExtensions.bodyAsCQ(existentialRule),
                instance
              )

              // because we are "reusing" local names, we can no longer
              // uniformly extend homomorphisms to existential variables
              // (i.e. local names to which existential variables are mapped depend on
              //  how frontier variables are mapped to local names, as those are the
              //  local names that get inherited to the child instance)
              bodyJoinResult.allHomomorphisms.flatMap((bodyHomomorphism) => {
                def foo(bodyHomomorphism: HomomorphicMapping[LocalInstanceTerm])
                  : IterableOnce[FormalInstance[LocalInstanceTerm]] = {

                  // The set of local names that are inherited from the parent instance
                  // to the child instance.
                  val inheritedLocalNames =
                    TGDExtensions.frontierVariables(existentialRule).map(bodyHomomorphism.apply)

                  // Names we can reuse (i.e. assign to existential variables in the rule)
                  // in the child instance. All names in this set should be considered distinct
                  // from the names in the parent instance having the same value, so we
                  // are explicitly ignoring the "implicit equality coding" semantics here.
                  val namesToReuseInChild =
                    (0 until maxArityOfAllPredicatesUsedInRules).map(LocalName(_))
                      .toSet.removedAll(inheritedLocalNames)
                      .toVector

                  val headVariableHomomorphism =
                    existentialVariables
                      .zipWithIndex
                      .map { (variable, index) => (variable, namesToReuseInChild(index)) }
                      .toMap

                  val extendedHomomorphism =
                    bodyHomomorphism.extendWithMapping(headVariableHomomorphism)

                  // The instance containing only the head atom produced by the existential rule.
                  // This should be a singleton instance because the existential rule is normal.
                  val headInstance =
                    FormalInstance.of(extendedHomomorphism.materializeFunctionFreeAtom(
                      headAtom,
                      LocalInstanceTerm.RuleConstant(_)
                    ))

                  // if names are not preserved, we reject this homomorphism
                  if (!namesToBePreservedDuringChase.toSet.subsetOf(inheritedLocalNames))
                    return Set.empty

                  // The set of facts in the parent instance that are
                  // "guarded" by the head of the existential rule.
                  // Those are precisely the facts that have its local names
                  // appearing in the head of the existential rule
                  // as a homomorphic image of a frontier variable in the rule.
                  val inheritedFactsInstance = instance.restrictToAlphabetsWith((term) =>
                    term.isConstantOrSatisfies(inheritedLocalNames.contains)
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
                foo(bodyHomomorphism)
              })
            }
            foo(existentialRule)
          }

          val children = saturatedRuleSet.existentialRules.flatMap(allChasesWithRule(_))
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
        shortcutChaseOneStep(_)
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
      val relevantSubquery = connectedConjunctiveQuery.subqueryRelevantToVariables(
        instance.coexistentialVariables.toSet
      ).get

      val instancesWithGuessedVariablesPreserved = chaseLocalInstance(
        instance.localInstance,
        // we need to preserve all local names in the range of localWitnessGuess and queryConstantEmbedding
        // because they are treated as special symbols corresponding to variables and query constants
        // occurring in the subquery.
        instance.localWitnessGuess.values.toSet ++ instance.queryConstantEmbedding.values
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
              val coveredVariables = extendedGuess.keySet
              val newlyCoveredAtoms =
                util.Arrays.stream(relevantSubquery.getAtoms).filter((atom: Atom) => {
                  val atomVariables = atom.getVariables.toSet
                  val allVariablesAreCovered = atomVariables.subsetOf(coveredVariables)

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
                relevantSubquery.connectedComponentsOf(
                  instance.coexistentialVariables.toSet -- newlyCoveredVariables
                )

              splitCoexistentialVariables.forall(splitCoexistentialVariablesComponent => {
                val newNeighbourhood =
                  relevantSubquery.strictNeighbourhoodOf(
                    splitCoexistentialVariablesComponent
                  ) -- instance.ruleConstantWitnessGuess.keySet

                // For the same reason as .get() call in the beginning of the method,
                // this .get() call succeeds.
                // noinspection OptionalGetWithoutIsPresent
                val newRelevantSubquery =
                  relevantSubquery
                    .subqueryRelevantToVariables(splitCoexistentialVariablesComponent)
                    .get

                val inducedInstance = new SubqueryEntailmentInstance(
                  instance.ruleConstantWitnessGuess,
                  splitCoexistentialVariablesComponent,
                  chasedInstance,
                  MapExtensions.restrictToKeys(extendedLocalWitnessGuess, newNeighbourhood),
                  MapExtensions.restrictToKeys(
                    instance.queryConstantEmbedding,
                    ConjunctiveQueryExtensions.allConstants(newRelevantSubquery)
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

    NormalizingDPTableSEEnumeration.allWellFormedNormalizedSubqueryEntailmentInstancesFor(
      extensionalSignature,
      ruleConstants,
      connectedConjunctiveQuery
    ).foreach(dpTable.fillTableUpto(_))

    dpTable.getKnownYesInstances
  }
  override def toString: String =
    "NormalizingDPTableSEEnumeration{" + "datalogSaturationEngine=" + datalogSaturationEngine + '}'
}
