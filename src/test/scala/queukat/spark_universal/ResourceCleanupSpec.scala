package queukat.spark_universal

import org.scalatest.funsuite.AnyFunSuite

class ResourceCleanupSpec extends AnyFunSuite {
  test("start can be invoked multiple times") {
    ResourceCleanup.start()
    ResourceCleanup.start()
    ResourceCleanup.stop()
  }
}
