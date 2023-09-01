package io.github.kory33.guardedqueries.core.subqueryentailments

import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import io.github.kory33.guardedqueries.core.fol.NormalGTGD
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery

/**
 * The interface to objects that can compute subquery entailment relations.
 *
 * An object conforming to this interface is able to produce an Iterable of
 * [[SubqueryEntailmentInstances]] when given a saturated rule set and a
 * bound-variable-connected query. Given a saturated rule set `S` and a bound-variable-connected
 * query `Q`, the Iterable produced by this object must satisfy the following three conditions:
 * <ol> <li> <b>Well-formedness of each item in the Iterable</b>
 *
 * If we write `k` for the maximum arity of predicates appearing in `S` and `Q`, each
 * [[SubqueryEntailmentInstance]] `i` in the Iterable must satisfy the following conditions: <ol>
 * <li> `i.ruleConstantWitnessGuess()` is a map from variables in `Q` to constants appearing in
 * `S` </li> <li> `i.coexistentialVariables()` is a nonempty set of bound variables in `Q` that
 * is connected in `Q` but disjoint from `i.ruleConstantWitnessGuess().keySet()` </li> <li>
 * `i.localInstance()` is a formal instance on <ul> <li>constants appearing in `Q`</li>
 * <li>local names from the set `{ 0,..., 2*k-1 ` }</li> </ul> such that at most `k` distinct
 * local names appear in the formal instance </li> <li> `i.localWitnessGuess()` is a map that
 * sends all `Q`-bound variables that are <ol> <li>in the strict neighbourhood of
 * `i.coexistentialVariables()` in `Q`, and</li> <li>not in
 * `i.ruleConstantWitnessGuess().keySet()`</li> </ol> to local names appearing in
 * `i.localInstance()` </li> <li> `i.queryConstantEmbedding()` is an injective map that send
 * <i>all</i> constants that <ol> <li>appear in a subquery of `Q` weakly induced by
 * `i.coexistentialVariables()` but</li> <li>do not appear in `S`</li> </ol> to local names that
 * are <ol> <li>active in `i.localInstance()` and</li> <li>not in the range of
 * `i.localWitnessGuess()`</li> </ol> </li> </ol> </li> <li> <b>Soundness</b>
 *
 * All [[SubqueryEntailmentInstance]]s in the output Iterable must represent an entailment
 * relation between the associated local instance and the subquery of `Q` that is induced in a
 * particular way.
 *
 * To be more precise, if `i` is an [[SubqueryEntailmentInstance]] in the output Iterable, then it
 * must be the case that the conjunction of <ul> <li> local instance `i.localInstance` regarded
 * as a conjunction of facts, except that local names appearing in the local instance are
 * considered as fresh constants </li> <li> rules `S` regarded as a conjunction of normal GTGDs
 * </li> </ul> implies the subquery of `Q` that <ul> <li>has `i.coexistentialVariables` as
 * existentially quantified variables</li> <li> has all atoms from `Q` that share at least one
 * variable with `i.coexistentialVariables`, except that variables not in
 * `i.coexistentialVariables` are substituted to rule constants or local names according to
 * `i.ruleConstantWitnessGuess` and `i.localWitnessGuess` </ul>
 *
 * For example, TODO: add a very concrete example here with a real entailment relation </li>
 * <li> <b>Covering property</b>
 *
 * If we write `outInstances` for the set of all [[SubqueryEntailmentInstance]]s in the output
 * Iterable and `soundInstances` for the set of all sound [[SubqueryEntailmentInstance]]s, it may
 * be the case that `outInstances` is a proper subset of `soundInstances`.
 *
 * However, we require that `outInstances` must be sufficiently large so that `outInstances`
 * covers all of `soundInstances` via the subsumption relation. In other words, we demand that
 * for each `si` in `soundInstances`, there exists `oi` in `outInstances` such that `oi`
 * subsumes `si`. </li> </ol>
 */
@FunctionalInterface trait SubqueryEntailmentEnumeration {
  def apply(extensionalSignature: FunctionFreeSignature,
            saturatedRuleSet: SaturatedRuleSet[? <: NormalGTGD],
            boundVariableConnectedQuery: ConjunctiveQuery
  ): Iterable[SubqueryEntailmentInstance]
}
