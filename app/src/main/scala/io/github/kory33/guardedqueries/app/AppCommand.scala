package io.github.kory33.guardedqueries.app

import uk.ac.ox.cs.gsat.GTGD
import uk.ac.ox.cs.pdq.fol.ConjunctiveQuery

enum AppCommand:
  case RegisterRule(rule: GTGD)
  case ShowRegisteredRules
  case AtomicRewriteRegisteredRules
  case Rewrite(query: ConjunctiveQuery, implChoice: AppCommand.EnumerationImplChoice)
  case Help

object AppCommand:
  enum EnumerationImplChoice:
    case Naive
    case Normalizing
    case DFSNormalizing
