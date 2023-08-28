package io.github.kory33.guardedqueries.core.utils.extensions

import java.util.function.Function

object FunctionExtensions {

  /**
   * A helper function that converts a lambda expression to a {@link Function} object. <p> This
   * function seems a bit mysterious, but the purpose of this function is to allow the use of
   * lambda expressions on the right hand side of the assignment when the type of the declared
   * variable should be inferred (i.e. when assignment uses {@code var}).
   */
  def asFunction[T, R](lambda: Function[_ >: T, _ <: R]): Function[T, R] = lambda.apply
}
