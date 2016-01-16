package de.zalando.aruha.nakadi

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OperationsScalaTest extends FlatSpec with MustMatchers {
  "Sum" must "sum two ints" in {
    OperationsScala.+(1, 2) must be (3)
  }
  "Sub" must "subtract two ints" in {
    OperationsScala.-(5, 2) must be (3)
  }
}
