package queukat.spark_universal

object MigrationLogging {
  private val Reset = "\u001b[0m"
  private val Bold = "\u001b[1m"
  private val Blue = "\u001b[34m"
  private val Cyan = "\u001b[36m"
  private val Green = "\u001b[32m"
  private val Yellow = "\u001b[33m"
  private val Red = "\u001b[31m"
  private val Magenta = "\u001b[35m"

  def stage(name: String): String = color(s"[$name]", Blue, bold = true)

  def success(name: String): String = color(s"[$name]", Green, bold = true)

  def warning(name: String): String = color(s"[$name]", Yellow, bold = true)

  def failure(name: String): String = color(s"[$name]", Red, bold = true)

  def kv(key: String, value: Any): String = s"${color(key, Cyan)}=$value"

  def sourceTable(owner: String, tableName: String): String = color(s"$owner.$tableName", Magenta, bold = true)

  def targetTable(tableName: String): String = color(tableName, Magenta, bold = true)

  def sqlPreview(sql: String, maxLength: Int = 180): String = {
    val normalized = Option(sql).map(_.replaceAll("\\s+", " ").trim).getOrElse("")
    val shortened =
      if (normalized.length <= maxLength) normalized
      else normalized.take(maxLength - 3) + "..."
    kv("sql", shortened)
  }

  def previewList(values: Seq[String], maxItems: Int = 8): String = {
    val preview = values.take(maxItems).mkString(", ")
    if (values.size <= maxItems) preview else s"$preview, ... (${values.size} total)"
  }

  def elapsedSince(startedAtNanos: Long): String = {
    val elapsedMillis = (System.nanoTime() - startedAtNanos) / 1000000L
    f"${elapsedMillis / 1000.0}%.2fs"
  }

  private def color(value: String, colorCode: String, bold: Boolean = false): String = {
    if (!isColorEnabled) {
      value
    } else {
      val weight = if (bold) Bold else ""
      s"$weight$colorCode$value$Reset"
    }
  }

  private def isColorEnabled: Boolean = {
    sys.props.get("spark.universal.log.color").flatMap(parseBoolean).getOrElse {
      sys.env.get("NO_COLOR").isEmpty && (Option(System.console()).nonEmpty || sys.env.contains("WT_SESSION") || sys.env.contains("TERM"))
    }
  }

  private def parseBoolean(value: String): Option[Boolean] = {
    value.trim.toLowerCase match {
      case "1" | "true" | "yes" | "on" => Some(true)
      case "0" | "false" | "no" | "off" => Some(false)
      case _ => None
    }
  }
}
