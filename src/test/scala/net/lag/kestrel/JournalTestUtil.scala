package net.lag.kestrel

import java.io._

object JournalTestUtil {
  def journalsForQueue(path: File, queueName: String): List[String] = {
    journalsForQueue(new LocalDirectory(path.getCanonicalPath, null), queueName)
  }
  def journalsForQueue(streamContainer: PersistentStreamContainer, queueName: String): List[String] = {
    Journal.archivedFilesForQueue(streamContainer, queueName) ++ List(queueName)
  }
  def journalsBefore(path: File, queueName: String, filename: String): Seq[String] = {
    journalsBefore(new LocalDirectory(path.getCanonicalPath, null), queueName, filename)
  }
  def journalsBefore(streamContainer: PersistentStreamContainer, queueName: String, filename: String): Seq[String] = {
    journalsForQueue(streamContainer, queueName).takeWhile { _ != filename }
  }
  def journalAfter(path: File, queueName: String, filename: String): Option[String] = {
    journalAfter(new LocalDirectory(path.getCanonicalPath, null), queueName, filename)
  }
  def journalAfter(streamContainer: PersistentStreamContainer, queueName: String, filename: String): Option[String] = {
    journalsForQueue(streamContainer, queueName).dropWhile { _ != filename }.drop(1).headOption
  }
}