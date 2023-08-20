package io.github.kory33.guardedqueries.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.github.kory33.guardedqueries.core.utils.extensions.SetLikeExtensions;
import io.github.kory33.guardedqueries.core.utils.extensions.VariableSetExtensions;
import org.javafp.data.IList;
import org.javafp.parsecj.Combinators;
import org.javafp.parsecj.Parser;
import org.javafp.parsecj.Text;
import org.javafp.parsecj.input.Input;
import uk.ac.ox.cs.gsat.GTGD;
import uk.ac.ox.cs.pdq.fol.*;

import java.util.Arrays;
import java.util.HashSet;

public class FormulaParsers {
    private FormulaParsers() {
    }

    // here is the grammar for formulae that we will deal with
    // (we preprocess all input strings and remove all spaces)
    //
    // term ::= <nonempty sequence of characters not including ',' nor ')'>
    // predicate ::= <nonempty sequence of characters not including '(' and not starting with 'EE' or '->'>
    // variable ::= <nonempty sequence of characters not including ',' or '.' which
    //               the parsing context does not judge as constant>
    //
    // terms ::= term | term ',' terms
    // atom ::= predicate '()' | predicate '(' terms ')'
    // atoms ::= atom | atom ',' atoms
    // variables ::= variable | variable ',' variables

    public static Parser<Character, Term> termParser(FormulaParsingContext context) {
        final var termChars = Combinators.<Character>satisfy(c -> c != ',' && c != ')');
        return termChars.many1().map(chars -> {
            final var termString = IList.listToString(chars);

            if (context.isConstantSymbol().test(termString)) {
                return context.mapConstantSymbolToConstant().apply(termString);
            } else {
                return Variable.create(termString);
            }
        });
    }

    public static final Parser<Character, String> predicateNameParser =
            Combinators.<Character>satisfy(c -> c != '(').many1().bind(chars -> {
                final var predicateString = IList.listToString(chars);

                if (predicateString.startsWith("EE") || predicateString.startsWith("->")) {
                    return Combinators.fail("predicate name cannot start with 'EE' or '->', found: " + predicateString);
                } else {
                    return Combinators.retn(predicateString);
                }
            });

    public static Parser<Character, Atom> atomParser(FormulaParsingContext context) {
        return predicateNameParser.bind(predicateName ->
                Text.chr('(').then(termParser(context).sepBy(Text.chr(','))).bind(terms ->
                        Text.chr(')').map(_ignored -> {
                            final var arity = terms.size();
                            final var predicate = Predicate.create(predicateName, arity);
                            return Atom.create(predicate, terms.stream().toArray(Term[]::new));
                        })
                )
        );
    }

    public static Parser<Character, Variable> variableParser(FormulaParsingContext context) {
        return Combinators
                .<Character>satisfy(c -> c != ',' && c != '.')
                .many1()
                .bind(chars -> {
                    final var variableString = IList.listToString(chars);
                    if (context.isConstantSymbol().test(variableString)) {
                        return Combinators
                                .fail("a constant symbol " + variableString + " cannot be bound in quantifier");
                    } else {
                        return Combinators.retn(Variable.create(variableString));
                    }
                });
    }

    public static Parser<Character, IList<Variable>> existentialVariableBindingsParser(FormulaParsingContext context) {
        return Text
                .string("EE")
                .then(variableParser(context).sepBy(Text.chr(',')))
                .bind(variables -> Text.chr('.').bind(_ignored -> {
                    final var variableList = ImmutableList.copyOf(variables.stream().iterator());

                    if (ImmutableSet.copyOf(variableList).size() != variableList.size()) {
                        return Combinators.fail("existential variables must be distinct, found " + variableList);
                    } else {
                        return Combinators.retn(variables);
                    }
                }));
    }

    // We use the following grammar for TGDs
    //
    // gtgdHead ::= atoms | 'EE' variables '.' atoms
    // gtgd ::= atoms '->' gtgdHead
    public static Parser<Character, TGD> tgdParser(FormulaParsingContext context) {
        record TGDHead(IList<Atom> headAtoms, IList<Variable> existentialVariables) {
        }

        final var existentialHeadParser = existentialVariableBindingsParser(context)
                .bind(existentialVariables ->
                        atomParser(context).sepBy(Text.chr(',')).map(headAtoms ->
                                new TGDHead(headAtoms, existentialVariables)
                        )
                );

        final var nonExistentialHeadParser = atomParser(context).sepBy(Text.chr(',')).map(headAtoms ->
                new TGDHead(headAtoms, IList.empty())
        );

        final var headParser = existentialHeadParser.or(nonExistentialHeadParser);

        return atomParser(context).sepBy(Text.chr(',')).bind(bodyAtoms -> Text.string("->").then(headParser).bind(head -> {
            final var bodyAtomsArray = bodyAtoms.stream().toArray(Atom[]::new);
            final var headAtoms = head.headAtoms.stream().toArray(Atom[]::new);
            final var tgd = TGD.create(bodyAtomsArray, headAtoms);

            final var declaredExistentialVariables = VariableSetExtensions.sortBySymbol(head.existentialVariables.toList());
            final var foundExistentialVariables = VariableSetExtensions.sortBySymbol(Arrays.asList(tgd.getExistential()));

            if (!foundExistentialVariables.equals(declaredExistentialVariables)) {
                return Combinators.fail("existential variables in head do not match those in body: " +
                        "declared: " + declaredExistentialVariables + ", found: " + foundExistentialVariables);
            } else {
                return Combinators.retn(tgd);
            }
        }));
    }

    public static Parser<Character, GTGD> gtgdParser(FormulaParsingContext context) {
        return tgdParser(context).map(tgd -> {
            final var bodyAtoms = new HashSet<>(Arrays.asList(tgd.getBodyAtoms()));
            final var headAtoms = new HashSet<>(Arrays.asList(tgd.getHeadAtoms()));
            return new GTGD(bodyAtoms, headAtoms);
        });
    }

    public static Parser<Character, ConjunctiveQuery> conjunctiveQueryParser(FormulaParsingContext context) {
        final var existentialQueryParser = existentialVariableBindingsParser(context).bind(existentialVariables ->
                atomParser(context).sepBy(Text.chr(',')).map(queryAtoms -> {
                    final var atomVariables = queryAtoms.stream()
                            .flatMap(atom -> Arrays.stream(atom.getFreeVariables()))
                            .toList();

                    final var freeVariables = SetLikeExtensions.difference(
                            atomVariables,
                            existentialVariables.toList()
                    ).toArray(Variable[]::new);

                    final var queryAtomsArray = queryAtoms.stream().toArray(Atom[]::new);

                    return ConjunctiveQuery.create(freeVariables, queryAtomsArray);
                })
        );

        final var nonExistentialQueryParser = atomParser(context).sepBy(Text.chr(',')).map(queryAtoms -> {
            final var atomVariables = queryAtoms.stream()
                    .flatMap(atom -> Arrays.stream(atom.getFreeVariables()))
                    .toList();
            final var freeVariables = atomVariables.toArray(Variable[]::new);
            final var queryAtomsArray = queryAtoms.stream().toArray(Atom[]::new);
            return ConjunctiveQuery.create(freeVariables, queryAtomsArray);
        });

        return existentialQueryParser.or(nonExistentialQueryParser);
    }

    public record WhitespaceIgnoringParser<FormulaClass>(Parser<Character, FormulaClass> parser) {
        public FormulaClass parse(String formulaString) {
            final var spaceRemoved = formulaString.replace(" ", "");
            try {
                return parser.parse(Input.of(spaceRemoved)).getResult();
            } catch (Exception e) {
                throw new IllegalArgumentException("invalid formula string: " + formulaString, e);
            }
        }
    }

    public record GTGDParser(FormulaParsingContext context) {
        public GTGD parse(String formulaString) {
            final var spaceRemoved = formulaString.replace(" ", "");
            try {
                return gtgdParser(context).parse(Input.of(spaceRemoved)).getResult();
            } catch (Exception e) {
                throw new IllegalArgumentException("invalid GTGD string: " + formulaString, e);
            }
        }
    }

    public record ConjunctiveQueryParser(FormulaParsingContext context) {
        public ConjunctiveQuery parse(String formulaString) {
            final var spaceRemoved = formulaString.replace(" ", "");
            try {
                return conjunctiveQueryParser(context).parse(Input.of(spaceRemoved)).getResult();
            } catch (Exception e) {
                throw new IllegalArgumentException("invalid conjunctive query string: " + formulaString, e);
            }
        }
    }
}
