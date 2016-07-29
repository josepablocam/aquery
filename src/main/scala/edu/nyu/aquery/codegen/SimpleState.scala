package edu.nyu.aquery.codegen

import scalaz._
import Scalaz._

import scalaz.Free.Trampoline

/**
 * Wrapper around state monad operations from scalaz. We don't plan on using scalaz much but in
 * this case, it made sense to work with their State monad, in case we run into stackoverflow
 * issues, we can leverage the ability to trampoline from the library.
 */
object SimpleState {
  def unit[S, A](a: A): State[S, A] = State.state[S,A](a)
  def get[S]: State[S, S] = State.get
  def read[S, R](r: S => R): State[S, R] = State.gets(r)
  def set[S](s: S): State[S, Unit] = State.put(s)
  def modify[S](f: S => S): State[S, Unit] = State.modify(f)
  def sequence[S, A](ls: List[State[S, A]]): State[S, List[A]] =
    ls.sequence[({type l[X]=State[S, X]})#l, A]
}




