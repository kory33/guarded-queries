package io.github.kory33.guardedqueries.core.formalinstance
import uk.ac.ox.cs.pdq.fol.Atom
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery
import uk.ac.ox.cs.pdq.fol.Variable
import uk.ac.ox.cs.pdq.fol.{Constant => PDQConstant}

type QueryLikeAtom[QueryVariable, Constant] =
  FormalFact[Either[QueryVariable, Constant]]

/**
 * We can think of a formal instance (over variables and constants) as a conjunctive query,
 * where conjuncts are represented by query-like atoms. Each query-like atom then represents an
 * atom in the conjunctive query, where an argument of type [[QueryVariable]] in [[Left]] are to
 * be matched with [[Constant]] in a given formal instance over [[Constant]]s.
 */
type QueryLikeInstance[QueryVariable, Constant] =
  FormalInstance[Either[QueryVariable, Constant]]

object CQAsFormalInstance {
  given Extensions: AnyRef with
    extension (functionFreeAtom: Atom)
      /**
       * Convert this atom into a query-like atom.
       *
       * @throws IllegalArgumentException
       *   if the given atom contains terms that are neither variables nor constants.
       */
      def asQueryLikeAtom[Constant: IncludesFolConstants]: QueryLikeAtom[Variable, Constant] =
        FormalFact(
          functionFreeAtom.getPredicate,
          functionFreeAtom.getTerms
            .map {
              case v: Variable    => Left(v)
              case c: PDQConstant => Right(IncludesFolConstants[Constant].includeConstant(c))
              case t =>
                throw IllegalArgumentException(
                  s"query contains a non-variable, non-constant term: $t"
                )
            }
            .toList
        )

    extension (nonExistentialQuery: ConjunctiveQuery)
      /**
       * Convert this conjunctive query into a query-like formal instance.
       *
       * @throws IllegalArgumentException
       *   if the given query contains existential variables or contains terms that are neither
       *   variables nor constants.
       */
      def asQueryLikeFormalInstance[Constant: IncludesFolConstants]
        : QueryLikeInstance[Variable, Constant] = {
        if (nonExistentialQuery.getBoundVariables.nonEmpty) {
          throw IllegalArgumentException(
            s"Query $nonExistentialQuery contains existential variables"
          )
        } else {
          FormalInstance {
            nonExistentialQuery
              .getAtoms
              .map(_.asQueryLikeAtom)
              .toSet
          }
        }
      }

}
