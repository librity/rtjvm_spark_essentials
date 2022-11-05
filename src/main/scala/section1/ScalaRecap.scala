package section1

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

object ScalaRecap extends App {
  //  Values and variables
  val bool: Boolean = true

  //  Expressions: evaluate to a value
  val ifExpression = if (2 > 3) "bigger" else "smaller"

  //  Instructions (Imperative) vs. Expressions (Functional)
  val theUnit: Unit = println("Hello Scala!")
  //  Unit = "no meaningful value", void

  //  Functions
  def myFunc(x: Int) = 42

  //  OOP
  class Animal

  class Dog extends Animal

  trait Carnivore {
    def eat(meal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(meal: Animal): Unit = println("Nhan nham")
  }

  //  Singleton pattern
  object MySingleton

  //  Class Companions
  object Carnivore


  //  Generics
  trait MyList[T]

  trait MyCovariantList[+T]


  //  Method notation
  val x = 1 + 2
  val y = 1.+(2)


  //  Functional Programming
  val incrementer: Function1[Int, Int] = new Function[Int, Int] {
    override def apply(x: Int): Int = x + 1
  }

  val incrementerV2: (Int) => Int = new ((Int) => Int) {
    override def apply(x: Int): Int = x + 1
  }

  val incrementerV3: (Int) => Int = x => x + 1

  println(incrementer(41))
  println(incrementerV2(41))
  println(incrementerV3(41))


  //  HOF: map, flatMap, filter
  val processedList = List(1, 2, 3).map(incrementerV3)
  println(processedList)


  //  Pattern Matching
  val unknown: Any = 42
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }



  //  Try-catch with Pattern Matching
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "some value"
    case _ => "something else"
  }


  //  Futures


  val aFuture = Future {
    //    Some expensive computation in a thread
    42
  }
  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"Meaning of life: $meaningOfLife")
    case Failure(exception) => println(s"Failure: $exception")
  }


  //  Partial Function
  val partialFunc = (x: Int) => x match {
    case 1 => 42
    case 8 => 84
    case _ => 123
  }
  println(partialFunc(8))

  val partialFuncV2: PartialFunction[Int, Int] = {
    case 1 => 42
    case 8 => 84
    case _ => 123
  }
  println(partialFuncV2(42))


  //  Implicits: auto-injection by the compiler
  def methodWthImplicitArg(implicit x: Int) = x + 43

  implicit val implicitInt = 67
  println(methodWthImplicitArg)


  //  Implicit Conversions - Implicit Functions/Methods
  //  Case Class: lightweight data structure with utility methods injected by the compiler.
  case class Person(name: String) {
    def greet() = println(s"Hello, my name is $name")
  }

  //  Won't compile if implicit conversions are ambiguous within the scope.
  //  implicit def stringToPersonV2(name: String) = Person(name)
  implicit def stringToPerson(name: String) = Person(name)


  "Bob".greet // stringToPerson("Bob").greet()


  //  Implicit Conversions - Implicit classes
  implicit class Cat(name: String) {
    def meow = println("Meow.")
  }

  //  Implicit Classes are preferable to Implicit Functions/Methods
  "Lassie".meow

  /**
   * Implicit Lookup Order
   * 1. Local Scope
   * 2. Imported Scope
   * 3. Companion Objects of the types involved in the method call.
   */

}


