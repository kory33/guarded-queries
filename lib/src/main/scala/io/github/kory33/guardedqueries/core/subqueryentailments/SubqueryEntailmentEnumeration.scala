package io.github.kory33.guardedqueries.core.subqueryentailments

import io.github.kory33.guardedqueries.core.fol.FunctionFreeSignature
import io.github.kory33.guardedqueries.core.fol.NormalGTGD
import io.github.kory33.guardedqueries.core.rewriting.SaturatedRuleSet
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery

/**
 * The interface to objects that can compute subquery entailment relations. <p> An object
 * conforming to this interface is able to produce a stream of {@link
 * SubqueryEntailmentInstance}s when given a saturated rule set and a bound-variable-connected
 * query. Given a saturated rule set {@code S} and a bound-variable-connected query {@code Q},
 * the stream produced by this object must satisfy the following three conditions: <ol> <li>
 * <b>Well-formedness of each item in the stream</b> <p> If we write {@code k} for the maximum
 * arity of predicates appearing in {@code S} and {@code Q}, each {@link
 * SubqueryEntailmentInstance} {@code i} in the stream must satisfy the following conditions:
 * <ol> <li> {@code i.ruleConstantWitnessGuess()} is a map from variables in {@code Q} to
 * constants appearing in {@code S} </li> <li> {@code i.coexistentialVariables()} is a nonempty
 * set of bound variables in {@code Q} that is connected in {@code Q} but disjoint from {@code
 * i.ruleConstantWitnessGuess().keySet()} </li> <li> {@code i.localInstance()} is a formal
 * instance on <ul> <li>constants appearing in {@code Q}</li> <li>local names from the set
 * {@code { 0,..., 2*k-1 } }</li> </ul> such that at most {@code k} distinct local names appear
 * in the formal instance </li> <li> {@code i.localWitnessGuess()} is a map that sends all
 * {@code Q}-bound variables that are <ol> <li>in the strict neighbourhood of {@code
 * i.coexistentialVariables()} in {@code Q}, and</li> <li>not in {@code
 * i.ruleConstantWitnessGuess().keySet()}</li> </ol> to local names appearing in {@code
 * i.localInstance()} </li> <li> {@code i.queryConstantEmbedding()} is an injective map that
 * send <i>all</i> constants that <ol> <li>appear in a subquery of {@code Q} weakly induced by
 * {@code i.coexistentialVariables()} but</li> <li>do not appear in {@code S}</li> </ol> to
 * local names that are <ol> <li>active in {@code i.localInstance()} and</li> <li>not in the
 * range of {@code i.localWitnessGuess()}</li> </ol> </li> </ol> </li> <li> <b>Soundness</b> <p>
 * All {@link SubqueryEntailmentInstance}s in the output stream must represent an entailment
 * relation between the associated local instance and the subquery of {@code Q} that is induced
 * in a particular way. <p> To be more precise, if {@code i} is an {@link
 * SubqueryEntailmentInstance} in the output stream, then it must be the case that the
 * conjunction of <ul> <li> local instance {@code i.localInstance} regarded as a conjunction of
 * facts, except that local names appearing in the local instance are considered as fresh
 * constants </li> <li> rules {@code S} regarded as a conjunction of normal GTGDs </li> </ul>
 * implies the subquery of {@code Q} that <ul> <li>has {@code i.coexistentialVariables} as
 * existentially quantified variables</li> <li> has all atoms from {@code Q} that share at least
 * one variable with {@code i.coexistentialVariables}, except that variables not in {@code
 * i.coexistentialVariables} are substituted to rule constants or local names according to
 * {@code i.ruleConstantWitnessGuess} and {@code i.localWitnessGuess} </ul> <p> For example,
 * TODO: add a very concrete example here with a real entailment relation </li> <li> <b>Covering
 * property</b> <p> If we write {@code outInstances} for the set of all {@link
 * SubqueryEntailmentInstance}s in the output stream and {@code soundInstances} for the set of
 * all sound {@link SubqueryEntailmentInstance}s, it may be the case that {@code outInstances}
 * is a proper subset of {@code soundInstances}. <p> However, we require that {@code
 * outInstances} must be sufficiently large so that {@code outInstances} covers all of {@code
 * soundInstances} via the subsumption relation. In other words, we demand that for each {@code
 * si} in {@code soundInstances}, there exists {@code oi} in {@code outInstances} such that
 * {@code oi} subsumes {@code si}. </li> </ol>
 */
@FunctionalInterface trait SubqueryEntailmentEnumeration {
  def apply(extensionalSignature: FunctionFreeSignature,
            saturatedRuleSet: SaturatedRuleSet[_ <: NormalGTGD],
            boundVariableConnectedQuery: ConjunctiveQuery
  ): IterableOnce[SubqueryEntailmentInstance]
}
