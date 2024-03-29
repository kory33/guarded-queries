package io.github.kory33.guardedqueries.core.rewriting

import io.github.kory33.guardedqueries.core.datalog.DatalogProgram
import io.github.kory33.guardedqueries.core.datalog.DatalogRewriteResult
import io.github.kory33.guardedqueries.core.fol.DatalogRule
import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import io.github.kory33.guardedqueries.core.fol.LocalVariableContext
import io.github.kory33.guardedqueries.core.fol.NormalGTGD
import io.github.kory33.guardedqueries.core.formalinstance.FormalInstance
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm
import io.github.kory33.guardedqueries.core.subqueryentailments.LocalInstanceTerm.LocalName
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentEnumeration
import io.github.kory33.guardedqueries.core.subqueryentailments.SubqueryEntailmentInstance
import io.github.kory33.guardedqueries.core.utils.extensions.ConjunctiveQueryExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.IterableExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.MapExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.SetExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.StringSetExtensions.given
import io.github.kory33.guardedqueries.core.utils.extensions.VariableIterableExtensions.given
import uk.ac.ox.cs.gsat.AbstractSaturation
import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.*

import scala.jdk.CollectionConverters.*

/**
 * A result of rewriting a single maximally bound-variable-connected component of the input
 * query.
 */
private case class BoundVariableConnectedComponentRewriteResult(
  goalAtom: Atom,
  goalDerivationRules: Set[DatalogRule]
)

/**
 * The algorithm to compute the Datalog program that is equivalent to the given set of guarded
 * rules and the given conjunctive query.
 *
 * Each object of this class makes use of a [[AbstractSaturation]] from Guarded-Saturation and a
 * [[SubqueryEntailmentEnumeration]] object. The former is used to compute the guarded
 * saturation of the given guarded rules, and the latter is used to compute the entailment
 * relation of "small test instances" to subqueries of the given query.
 *
 * We convert each outcome of [[SubqueryEntailmentEnumeration]] into a Datalog rule deriving a
 * "subgoal", which is then combined into the final goal predicate using what we call the
 * "subgoal binding rule".
 *
 * Depending on the implementation of the [[SubqueryEntailmentEnumeration]] used, the outcome of
 * the rewriting could be huge, so it is highly recommended to "minimize" the outcome using
 * [[DatalogRewriteResult#minimizeSubgoalDerivationRulesUsing]] before running the output
 * program on a database instance.
 */
case class GuardedRuleAndQueryRewriter(
  saturation: AbstractSaturation[? <: GTGD],
  subqueryEntailmentEnumeration: SubqueryEntailmentEnumeration
) {

  /**
   * Transforms a subquery entailment into a rule to derive a subgoal.
   *
   * The instance `subqueryEntailment` must be a subquery entailment associated to some rule-set
   * (which we will not make use of in this method) and the query `subgoalAtoms.query()`.
   *
   * For example, suppose that the subquery entailment instance <pre><{ x ↦ a }, {z, w}, { y ↦ 2
   * }, { { R(2,1,3), U(1), P(2,c) } }></pre> where c is a constant from the rule-set, entails
   * the subquery of `subgoalAtoms.query()` relevant to {z, w}. Then we must add a rule of the
   * form <pre>R(y,_f1,_f3) ∧ U(_f1) ∧ P(y,c) → SGL_{z,w}(a,y)</pre> where `_f1` and `_f3` are
   * fresh variables and `SGL_{z,w`(x,y)} is the subgoal atom provided by subgoalAtoms object.
   *
   * In general, for each subquery entailment instance `<C, V, L, I>`, we need to produce a rule
   * of the form <pre> (I with each local name pulled back and unified by L, except that local
   * names outside the range of L are consistently replaced by fresh variables) → (the subgoal
   * atom corresponding to V, except that the same unification (by L) done to the premise is
   * performed and the variables in C are replaced by their preimages (hence some constant) in
   * C) </pre>
   */
  private def subqueryEntailmentToSubgoalRule(
    subqueryEntailment: SubqueryEntailmentInstance,
    subgoalAtoms: SubgoalAtomGenerator
  ): DatalogRule = {
    val ruleConstantWitnessGuess = subqueryEntailment.ruleConstantWitnessGuess
    val coexistentialVariables = subqueryEntailment.coexistentialVariables
    val localWitnessGuess = subqueryEntailment.localWitnessGuess
    val localInstance = subqueryEntailment.localInstance
    val queryConstantEmbeddingInverse = subqueryEntailment.queryConstantEmbedding.inverse

    val activeLocalNames = localInstance.getActiveTermsIn[LocalName]

    // We prepare a variable context that is closed within the rule
    // we are about to generate. This is essential to reduce the memory usage
    // of generated rule set, because this way we are more likely to
    // generate identical atoms which can be cached.
    val ruleLocalVariableContext = new LocalVariableContext("x_")

    // Mapping of local names to their preimages in the neighbourhood mapping.
    // Contains all active local names in the key set,
    // and the range of the mapping is a partition of domain of localWitnessGuess.
    val neighbourhoodPreimages: Map[LocalName, Set[Variable]] =
      localWitnessGuess.preimages(activeLocalNames)

    // unification of variables mapped by localWitnessGuess to fresh variables
    val unification: Map[Variable, Variable] = {
      for {
        equivalenceClass <- neighbourhoodPreimages.values
        unifiedVariable = ruleLocalVariableContext.getFreshVariable
        variable <- equivalenceClass
      } yield (variable, unifiedVariable)
    }.toMap

    val mappedInstance: FormalInstance[Term] = {
      // Mapping of local names to variables (or constants for local names bound to query-constant).
      // Contains all active local names in the key set.
      val nameToTermInRule = activeLocalNames.associate(localName =>
        neighbourhoodPreimages.get(localName) match {
          case None =>
            queryConstantEmbeddingInverse.asMap.get(localName) match {
              case Some(queryConstant) =>
                // if this local name is bound to a query constant,
                // we assign the query constant to the local name
                queryConstant
              case None =>
                // the local name is bound neither to a query constant nor
                // query-bound variable, so we assign a fresh variable to it
                ruleLocalVariableContext.getFreshVariable
            }
          case Some(equivalenceClass) =>
            // the contract of SubqueryEntailmentEnumeration guarantees that
            // local names bound to bound variables should not be bound
            // to a query constant
            assert(!queryConstantEmbeddingInverse.asMap.contains(localName))

            // otherwise unify to the variable corresponding to the preimage
            // e.g. if {x, y} is the preimage of localName and _xy is the variable
            // corresponding to {x, y}, we turn localName into _xy
            unification(equivalenceClass.head)
        }
      )

      localInstance.map(t => t.mapLocalNamesToTerm(nameToTermInRule(_)))
    }

    val mappedSubgoalAtom: Atom = {
      val subgoalAtom = subgoalAtoms.apply(coexistentialVariables)
      val orderedNeighbourhoodVariables = subgoalAtom.getTerms.map((term: Term) =>
        // this cast is safe, since only variables are applied to subgoal atoms
        term.asInstanceOf[Variable]
      )

      val neighbourhoodVariableToTermInRule: Variable => Term = variable => {
        if (unification.contains(variable)) unification(variable)
        else if (ruleConstantWitnessGuess.contains(variable))
          ruleConstantWitnessGuess(variable).constant
        else
          // The contract ensures that the given subquery entailment instance is a valid instance
          // with respect to the whole query (subgoalAtoms.query()), which means that
          // the neighbourhood of coexistential variables must be covered
          // by the union of domains of localWitnessGuess and ruleConstantWitnessGuess.
          throw new AssertionError(
            s"Variable $variable is not mapped by either unification or ruleConstantWitnessGuess"
          )
      }
      val replacedTerms =
        orderedNeighbourhoodVariables.map(neighbourhoodVariableToTermInRule(_))

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
    // Hence, the rule (mappedInstance → mappedSubgoalAtom) is a well-formed Datalog rule.
    DatalogRule(mappedInstance.asAtoms.toArray, Array(mappedSubgoalAtom))
  }

  /**
   * Rewrite a bound-variable-connected query `boundVariableConnectedQuery` into a pair of <ol>
   * <li>a fresh goal atom for the query.</li> <li>a set of additional rules that, when run on a
   * saturated base data, produces all answers to `boundVariableConnectedQuery`</li> </ol>
   */
  private def rewriteBoundVariableConnectedComponent(
    extensionalSignature: FunctionFreeSignature,
    saturatedRules: SaturatedRuleSet[? <: NormalGTGD],
    /* bound-variable-connected */ boundVariableConnectedQuery: ConjunctiveQuery,
    intentionalPredicatePrefix: String
  ) = {
    val queryGoalAtom: Atom = {
      val queryFreeVariables = boundVariableConnectedQuery.getFreeVariables.toSet

      Atom.create(
        Predicate.create(s"${intentionalPredicatePrefix}_GOAL", queryFreeVariables.size),
        queryFreeVariables.sortBySymbol: _*
      )
    }

    val subgoalAtoms =
      SubgoalAtomGenerator(boundVariableConnectedQuery, s"${intentionalPredicatePrefix}_SGL")

    val subgoalDerivationRules = subqueryEntailmentEnumeration(
      extensionalSignature,
      saturatedRules,
      boundVariableConnectedQuery
    ).map(subqueryEntailmentToSubgoalRule(_, subgoalAtoms))

    val subgoalGlueingRules = boundVariableConnectedQuery
      .getBoundVariables
      .toSet
      .powerset
      .map(existentialWitnessCandidate => {
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
        val baseWitnessVariables =
          boundVariableConnectedQuery.allVariables -- existentialWitnessCandidate

        val baseWitnessJoinConditions =
          boundVariableConnectedQuery
            .strictlyInduceSubqueryByVariables(baseWitnessVariables)
            .map(_.getAtoms)
            .getOrElse(Array.empty[Atom])

        val neighbourhoodsSubgoals = boundVariableConnectedQuery
          .connectedComponentsOf(existentialWitnessCandidate)
          .map(subgoalAtoms.apply)

        baseWitnessJoinConditions ++ neighbourhoodsSubgoals
      })
      .map(ruleBody => DatalogRule(ruleBody, Array(queryGoalAtom)))
      .toList

    BoundVariableConnectedComponentRewriteResult(
      queryGoalAtom,
      subgoalDerivationRules.toSet ++ subgoalGlueingRules
    )
  }

  /**
   * Compute a Datalog rewriting of a finite set of GTGD rules and a conjunctive query.
   *
   * Variables in the goal atom of the returned [[DatalogRewriteResult]] do correspond to the
   * free variables of the input query.
   */
  def rewrite(rules: Set[GTGD], query: ConjunctiveQuery): DatalogRewriteResult = {
    // Set of predicates that may appear in the input database.
    // Any predicate not in this signature can be considered as intentional predicates
    // and may be ignored in certain cases, such as when generating "test" instances.
    val extensionalSignature = FunctionFreeSignature.encompassingRuleQuery(rules, query)

    val intentionalPredicatePrefix = extensionalSignature.predicateNames
      .freshPrefixStartingWith(
        // stands for Intentional Predicates
        "IP"
      )

    val saturatedRuleSet = {
      val normalizedRules = NormalGTGD.normalize(
        rules,
        // stands for Normalization-Intermediate predicates
        s"${intentionalPredicatePrefix}_NI"
      )

      new SaturatedRuleSet[NormalGTGD](saturation, normalizedRules)
    }
    val goalAtom = {
      val deduplicatedQueryVariables = query.getFreeVariables.toSet
      val goalPredicate = Predicate.create(
        s"${intentionalPredicatePrefix}_GOAL",
        deduplicatedQueryVariables.size
      )

      Atom.create(goalPredicate, deduplicatedQueryVariables.toArray: _*)
    }

    val allGoalDerivationRules = {
      val cqConnectedComponents = CQBoundVariableConnectedComponents(query)

      // List of rewrite results of each maximally connected subquery
      val bvccRewriteResults = cqConnectedComponents.maximallyConnectedSubqueries
        .zipWithIndex
        .map((maximallyConnectedSubquery, index) =>
          rewriteBoundVariableConnectedComponent(
            extensionalSignature,
            saturatedRuleSet,
            maximallyConnectedSubquery,
            // prepare a prefix for intentional predicates that may be introduced to rewrite a
            // maximally connected subquery. "SQ" stands for "subquery".
            s"${intentionalPredicatePrefix}_SQ$index"
          )
        )
        .toList

      // the rule to "join" all subquery results
      val subgoalBindingRule: TGD = {
        // we have to join all of
        //  - bound-variable-free atoms
        //  - goal predicates of each maximally connected subquery
        val bodyAtoms =
          cqConnectedComponents.boundVariableFreeAtoms.toArray ++
            bvccRewriteResults.map(_.goalAtom)

        // ... to derive the final goal predicate
        TGD.create(bodyAtoms, Array(goalAtom))
      }

      bvccRewriteResults.flatMap(_.goalDerivationRules) :+ subgoalBindingRule
    }

    DatalogRewriteResult(
      DatalogProgram.tryFromDependencies(saturatedRuleSet.saturatedRules),
      DatalogProgram.tryFromDependencies(allGoalDerivationRules),
      goalAtom
    )
  }
}
