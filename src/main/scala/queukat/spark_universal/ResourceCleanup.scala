package queukat.spark_universal

import org.slf4j.LoggerFactory

import java.lang.ref.ReferenceQueue

/**
 * A singleton object providing functionality for automatic cleanup of resources,
 * specifically of [[ResultSetIterator]] objects. It maintains a thread which waits
 * for [[ResultSetIterator]] objects to be queued for cleanup, upon which it clears the references.
 */
object ResourceCleanup {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // A ReferenceQueue to hold PhantomReference(s) of ResultSetIterator objects
  val queue = new ReferenceQueue[ResultSetIterator]()

  /**
   * A thread which runs indefinitely, waiting for PhantomReferences to be added to the queue.
   * Once a reference is added, it clears the reference.
   */
  private val cleanerThread = new Thread(() => {
    try {
      while (true) {
        val ref = queue.remove()
        ref.clear()
        logger.info("Resource cleaned up successfully.")
      }
    } catch {
      case e: InterruptedException => logger.info("Resource cleanup thread interrupted.")
    }
  })

  /**
   * Starts the [[cleanerThread]].
   * It should be called to start the automatic resource cleanup.
   */
  def start(): Unit = {
    logger.info("Starting resource cleanup thread.")
    cleanerThread.start()
  }

  /**
   * Stops the [[cleanerThread]]. Should be called to stop the automatic resource cleanup,
   * typically when the cleanup is no longer required or the application is shutting down.
   */
  def stop(): Unit = {
    logger.info("Stopping resource cleanup thread.")
    cleanerThread.interrupt()
  }
}