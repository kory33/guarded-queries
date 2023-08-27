package io.github.kory33.guardedqueries.core.rewriting

import com.google.common.collect.ImmutableCollection
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import io.github.kory33.guardedqueries.core.datalog.{DatalogProgram, DatalogRewriteResult}
import io.github.kory33.guardedqueries.core.fol.DatalogRule
import io.github.kory33.guardedqueries.core.fol.NormalGTGD
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentEnumeration
import io.github.kory33.guardedqueries.core.utils.extensions.*
import org.apache.commons.lang3.tuple.Pair
import uk.ac.ox.cs.gsat.AbstractSaturation
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.*

import java.util
import java.util.stream.Stream

/**
 * The algorithm to compute the Datalog program that is equivalent to the given set of guarded
 * rules and the given conjunctive query. <p> Each object of this class makes use of a {@link
 * AbstractSaturation} from Guarded-Saturation and a {@link SubqueryEntailmentEnumeration}
 * object. The former is used to compute the guarded saturation of the given guarded rules, and
 * the latter is used to compute the entailment relation of "small test instances" to subqueries
 * of the given query. <p> We convert each outcome of {@link SubqueryEntailmentEnumeration} into
 * a Datalog rule deriving a "subgoal", which is then combined into the final goal predicate
 * using what we call the "subgoal binding rule". <p> Depending on the implementation of the
 * {@link SubqueryEntailmentEnumeration} used, the outcome of the rewriting could be huge, so it
 * is highly recommended to "minimize" the outcome using {@link
 * DatalogRewriteResult#minimizeSubgoalDerivationRulesUsing} before running the output program
 * on a database instance.
 */
object GuardedRuleAndQueryRewriter {

  /**
   * A result of rewriting a single maximally bound-variable-connected component of the input
   * query.
   */
  private case class BoundVariableConnectedComponentRewriteResult(
    goalAtom: Atom,
    goalDerivationRules: ImmutableCollection[_ <: DatalogRule]
  )
}

case class GuardedRuleAndQueryRewriter(
  saturation: AbstractSaturation[_ <: GTGD],
  subqueryEntailmentEnumeration: SubqueryEntailmentEnumeration
) {

  /**
   * Transforms a subquery entailment into a rule to derive a subgoal. <p> The instance {@code
   * subqueryEntailment} must be a subquery entailment associated to some rule-set (which we
   * will not make use of in this method) and the query {@code subgoalAtoms.query()}. <p> For
   * example, suppose that the subquery entailment instance <pre><{ x ↦ a }, {z, w}, { y ↦ 2 },
   * { { R(2,1,3), U(1), P(2,c) } }></pre> where c is a constant from the rule-set, entails the
   * subquery of {@code subgoalAtoms.query()} relevant to {z, w}. Then we must add a rule of the
   * form <pre>R(y,_f1,_f3) ∧ U(_f1) ∧ P(y,c) → SGL_{z,w}(a,y)</pre> where {@code _f1} and
   * {@code _f3} are fresh variables and {@code SGL_{z,w}(x,y)} is the subgoal atom provided by
   * subgoalAtoms object. <p> In general, for each subquery entailment instance {@code <C, V, L,
   * I>}, we need to produce a rule of the form <pre> (I with each local name pulled back and
   * unified by L, except that local names outside the range of L are consistently replaced by
   * fresh variables) → (the subgoal atom corresponding to V, except that the same unification
   * (by L) done to the premise is performed and the variables in C are replaced by their
   * preimages (hence some constant) in C) </pre>
   */
  private def subqueryEntailmentRecordToSubgoalRule(subqueryEntailment: Nothing,
                                                    subgoalAtoms: SubgoalAtomGenerator
  ) = {
    val ruleConstantWitnessGuess = subqueryEntailment.ruleConstantWitnessGuess
    val coexistentialVariables = subqueryEntailment.coexistentialVariables
    val localWitnessGuess = subqueryEntailment.localWitnessGuess
    val localInstance = subqueryEntailment.localInstance
    val queryConstantEmbeddingInverse = subqueryEntailment.queryConstantEmbedding.inverse

    // We prepare a variable context that is closed within the rule
    // we are about to generate. This is essential to reduce the memory usage
    // of generated rule set, because this way we are more likely to
    // generate identical atoms which can be cached.
    val ruleLocalVariableContext = new Nothing("x_")
    val activeLocalNames = localInstance.getActiveTerms.stream.flatMap {
      case t1: LocalInstanceTerm.LocalName => Stream.of(t1)
      case _                               => Stream.empty
    }.toList

    // Mapping of local names to their preimages in the neighbourhood mapping.
    // Contains all active local names in the key set,
    // and the range of the mapping is a partition of domain of localWitnessGuess.
    val neighbourhoodPreimages = MapExtensions.preimages(localWitnessGuess, activeLocalNames)

    // unification of variables mapped by localWitnessGuess to fresh variables
    val unification: ImmutableMap[Variable, Variable] = {
      val unificationMapBuilder = ImmutableMap.builder[Variable, Variable]
      for (equivalenceClass <- neighbourhoodPreimages.values) {
        val unifiedVariable = ruleLocalVariableContext.getFreshVariable
        import scala.collection.JavaConversions._
        for (variable <- equivalenceClass) {
          unificationMapBuilder.put(variable, unifiedVariable)
        }
      }
      unificationMapBuilder.build
    }

    // Mapping of local names to variables (or constants for local names bound to query-constant).
    // Contains all active local names in the key set.
    val nameToTermMap = ImmutableMapExtensions.consumeAndCopy(StreamExtensions.associate(
      activeLocalNames.stream,
      (localName: T) => {
        val preimage = neighbourhoodPreimages.get(localName)
        if (preimage.isEmpty) {
          if (queryConstantEmbeddingInverse.containsKey(localName)) {
            // if this local name is bound to a query constant,
            // we assign the query constant to the local name
            queryConstantEmbeddingInverse.get(localName)
          } else {
            // the local name is bound neither to a query constant nor
            // query-bound variable, so we assign a fresh variable to it
            ruleLocalVariableContext.getFreshVariable
          }
        } else {
          // the contract of SubqueryEntailmentEnumeration guarantees that
          // local names bound to bound variables should not be bound
          // to a query constant
          assert(!queryConstantEmbeddingInverse.containsKey(localName))

          // otherwise unify to the variable corresponding to the preimage
          // e.g. if {x, y} is the preimage of localName and _xy is the variable
          // corresponding to {x, y}, we turn localName into _xy
          val unifiedVariable = unification.get(preimage.iterator.next)
          assert(unifiedVariable != null)
          unifiedVariable
        }
      }
    ).iterator)

    val mappedInstance = localInstance.map((t) => t.mapLocalNamesToTerm(nameToTermMap.get))
    val mappedSubgoalAtom: Atom = {
      val subgoalAtom = subgoalAtoms.apply(coexistentialVariables)
      val orderedNeighbourhoodVariables = util.Arrays.stream(subgoalAtom.getTerms).map(
        (term: Term) =>
          term.asInstanceOf[
            Variable
          ] /* safe, since only variables are applied to subgoal atoms */
      )
      val neighbourhoodVariableToTerm = (variable: Variable) => {
        if (unification.containsKey(variable)) unification.get(variable)
        else if (ruleConstantWitnessGuess.containsKey(variable))
          ruleConstantWitnessGuess.get(variable)
        else {
          // The contract ensures that the given subquery entailment instance is a valid instance
          // with respect to the whole query (subgoalAtoms.query()), which means that
          // the neighbourhood of coexistential variables must be covered
          // by the union of domains of localWitnessGuess and ruleConstantWitnessGuess.
          throw new AssertionError(
            "Variable " + variable + " is not mapped by either unification or ruleConstantWitnessGuess"
          )
        }
      }
      val replacedTerms =
        orderedNeighbourhoodVariables.map(neighbourhoodVariableToTerm).toArray

      Atom.create(subgoalAtom.getPredicate, replacedTerms: _*)
    }

    // subgoalAtom has variables in the neighbourhood of coexistentialVariables as its parameters.
    // On the other hand, every variable in the neighbourhood of coexistentialVariables is mapped
    // either
    //  1. by ruleConstantWitnessGuess to a constant appearing in the rule, or
    //  2. by localWitnessGuess to a local name active in localInstance, which is then unified by unification,
    // and these mappings are applied uniformly across localInstance and subgoalAtom.
    //
    // Therefore, every variable appearing in mappedSubgoalAtom is a variable produced by unification map,
    // which must also occur in some atom of mappedInstance (as the local name
    // to which the unified variables were sent was active in localInstance).
    // Hence, the rule (mappedInstance → mappedSubgoalAtom) is a Datalog rule.
    DatalogRule(
      FormalInstance.asAtoms(mappedInstance).toArray,
      Array[Atom](mappedSubgoalAtom)
    )
  }

  /**
   * Rewrite a bound-variable-connected query {@code boundVariableConnectedQuery} into a pair of
   * <ol> <li>a fresh goal atom for the query.</li> <li>a set of additional rules that, when run
   * on a saturated base data, produces all answers to {@code boundVariableConnectedQuery}</li>
   * </ol>
   */
  private def rewriteBoundVariableConnectedComponent(
    extensionalSignature: FunctionFreeSignature,
    saturatedRules: SaturatedRuleSet[_ <: NormalGTGD],
    /* bound-variable-connected */ boundVariableConnectedQuery: ConjunctiveQuery,
    intentionalPredicatePrefix: String
  ) = {
    val queryGoalAtom: Atom = {
      val queryFreeVariables = boundVariableConnectedQuery.getFreeVariables
      val goalPredicate = Predicate.create(
        intentionalPredicatePrefix + "_GOAL",
        ImmutableSet.copyOf(queryFreeVariables).size
      )
      val sortedFreeVariables =
        VariableSetExtensions.sortBySymbol(util.Arrays.asList(queryFreeVariables)).toArray
      Atom.create(goalPredicate, sortedFreeVariables: _*)
    }

    val subgoalAtoms =
      new SubgoalAtomGenerator(boundVariableConnectedQuery, intentionalPredicatePrefix + "_SGL")
    val subgoalDerivationRules = subqueryEntailmentEnumeration.apply(
      extensionalSignature,
      saturatedRules,
      boundVariableConnectedQuery
    ).map((subqueryEntailment) =>
      subqueryEntailmentRecordToSubgoalRule(subqueryEntailment, subgoalAtoms)
    ).toList
    val subgoalGlueingRules = SetLikeExtensions.powerset(
      util.Arrays.asList(boundVariableConnectedQuery.getBoundVariables)
    ).map((existentialWitnessCandidate: ImmutableSet[Variable]) => {

      // A single existentialWitnessCandidate is a set of variables that the rule
      // (which we are about to produce) expects to be existentially satisfied.
      //
      // We call the complement of existentialWitnessCandidate as baseWitnessVariables,
      // since we expect (within the rule we are about to produce) those variables to be witnessed
      // by values in the base instance.
      //
      // The rule that we need to produce, therefore, will be of the form
      //   (subquery of boundVariableConnectedQuery strongly induced by baseWitnessVariables,
      //    except we turn all existential quantifications to universal quantifications)
      // ∧ (for each connected component V of existentialWitnessCandidate,
      //    a subgoal atom corresponding to V)
      //  → queryGoalAtom
      //
      // In the following code, we call the first conjunct of the rule "baseWitnessJoinConditions",
      // the second conjunct "neighbourhoodsSubgoals".
      val baseWitnessVariables = SetLikeExtensions.difference(
        ConjunctiveQueryExtensions.variablesIn(boundVariableConnectedQuery),
        existentialWitnessCandidate
      )
      val baseWitnessJoinConditions =
        ConjunctiveQueryExtensions.strictlyInduceSubqueryByVariables(
          boundVariableConnectedQuery,
          baseWitnessVariables
        ).map(_.getAtoms).orElseGet(() => new Array[Atom](0))
      val neighbourhoodsSubgoals = ConjunctiveQueryExtensions.connectedComponents(
        boundVariableConnectedQuery,
        existentialWitnessCandidate
      ).map(subgoalAtoms.apply).toArray()

      new DatalogRule(
        Stream.concat(
          util.Arrays.stream(baseWitnessJoinConditions),
          util.Arrays.stream(neighbourhoodsSubgoals)
        ).toArray(),
        Array[Atom](queryGoalAtom)
      )

    }).toList

    GuardedRuleAndQueryRewriter.BoundVariableConnectedComponentRewriteResult(
      queryGoalAtom,
      SetLikeExtensions.union(subgoalDerivationRules, subgoalGlueingRules)
    )
  }

  /**
   * Compute a Datalog rewriting of a finite set of GTGD rules and a conjunctive query. <p>
   * Variables in the goal atom of the returned {@link DatalogRewriteResult} do correspond to
   * the free variables of the input query.
   */
  def rewrite(rules: util.Collection[_ <: GTGD], query: ConjunctiveQuery): Nothing = {
    // Set of predicates that may appear in the input database.
    // Any predicate not in this signature can be considered as intentional predicates
    // and may be ignored in certain cases, such as when generating "test" instances.
    val extensionalSignature = FunctionFreeSignature.encompassingRuleQuery(rules, query)
    val intentionalPredicatePrefix = StringSetExtensions.freshPrefix(
      extensionalSignature.predicateNames, // stands for Intentional Predicates
      "IP"
    )

    val normalizedRules = NormalGTGD.normalize(
      rules, // stands for Normalization-Intermediate predicates
      intentionalPredicatePrefix + "_NI"
    )

    val saturatedRuleSet = new SaturatedRuleSet[GTGD](saturation, normalizedRules)
    val cqConnectedComponents = new CQBoundVariableConnectedComponents(query)

    val bvccRewriteResults = StreamExtensions.zipWithIndex(
      cqConnectedComponents.maximallyConnectedSubqueries.stream
    ).map(pair => {
      val maximallyConnectedSubquery = pair.getLeft

      // prepare a prefix for intentional predicates that may be introduced to rewrite a
      // maximally connected subquery. "SQ" stands for "subquery".
      val subqueryIntentionalPredicatePrefix =
        intentionalPredicatePrefix + "_SQ" + pair.getRight

      this.rewriteBoundVariableConnectedComponent(
        extensionalSignature,
        saturatedRuleSet,
        maximallyConnectedSubquery,
        subqueryIntentionalPredicatePrefix
      )
    }).toList

    val deduplicatedQueryVariables =
      ImmutableList.copyOf(ImmutableSet.copyOf(query.getFreeVariables))

    val goalPredicate =
      Predicate.create(intentionalPredicatePrefix + "_GOAL", deduplicatedQueryVariables.size)

    val goalAtom = Atom.create(goalPredicate, deduplicatedQueryVariables.toArray(`new`))

    // the rule to "join" all subquery results
    val subgoalBindingRule: TGD = {
      // we have to join all of
      //  - bound-variable-free atoms
      //  - goal predicates of each maximally connected subquery
      val bodyAtoms = Stream.concat(
        cqConnectedComponents.boundVariableFreeAtoms.stream,
        bvccRewriteResults.stream.map(_.goalAtom)
      ).toArray

      // ... to derive the final goal predicate
      TGD.create(bodyAtoms, Array[Atom](goalAtom))
    }

    val allDerivationRules = ImmutableSet.builder[TGD].addAll(
      bvccRewriteResults.stream.map(_.goalDerivationRules).flatMap(
        util.Collection.stream
      ).toList
    ).add(subgoalBindingRule).build

    new DatalogRewriteResult(
      DatalogProgram.tryFromDependencies(saturatedRuleSet.saturatedRules),
      DatalogProgram.tryFromDependencies(allDerivationRules),
      goalAtom
    )
  }

  override def toString: String =
    "GuardedRuleAndQueryRewriter{" + "saturation=" + saturation + ", subqueryEntailmentEnumeration=" + subqueryEntailmentEnumeration + '}'
}
