package net.lag.kestrel.tools

import java.io._
import java.nio.charset.Charset
import com.petebevin.markdown.MarkdownProcessor

object PublishSite {
  val charset = Charset.forName("UTF-8")

  def usage(errorMessage: Option[String] = None) {
    errorMessage.foreach { m => println("error: %s".format(m)) }
    println("PublishSite [--template=html-template-path] <markdown-file> [...]")
    System.exit(1)
  }

  def usage(errorMessage: String) { usage(Some(errorMessage)) }
  def usage(error: Throwable) { usage(Some(error.toString)) }

  def main(args: Array[String]) {
    val argsList = args.toList
    if (argsList.exists { a => a == "-h" || a == "--help" }) {
      usage()
    }

    val (templates, remainingArgs) = argsList.partition { a => a.startsWith("--template=") }

    val templateStream = templates.map { _.split("=", 2).lastOption } match {
      case Some(t) :: Nil if t == "" =>
        usage("no template given")
        None
      case Some(t) :: Nil =>
        try {
          Some(new FileInputStream(t))
        } catch { case e =>
          usage(e)
          None
        }
      case Nil => Some(getClass.getResourceAsStream("/markdown.template"))
      case _ =>
        usage("multiple templates given")
        None
    }

    val template = templateStream match {
      case Some(s) => streamToString(s)
      case None => usage("template error"); "" // unreachable
    }

    if (remainingArgs.isEmpty) usage("no markdown files given")

    remainingArgs.foreach { file => process(template, file) }
  }

  def streamToString(stream: InputStream): String = {
    val reader = new BufferedReader(new InputStreamReader(stream, charset))
    val builder = new StringBuilder()
    while(Option(reader.readLine) match {
      case Some(line) => builder.append(line); builder.append("\n"); true
      case _ => false
    }) { }
    stream.close()
    builder.toString
  }

  def process(template: String, sourcePath: String) {
    val sourceFile = new File(sourcePath)
    if (!sourceFile.exists) usage("file not found: %s".format(sourcePath))

    val destinationFilename =
      if (List("md", "markdown").contains(sourceFile.getName.split('.').last)) {
        sourceFile.getName.split('.').dropRight(1).mkString(".")
      } else {
        sourceFile.getName
      }

    val destinationFile = new File(sourceFile.getParent, destinationFilename + ".html")

    print("converting %s to %s...".format(sourceFile.getName, destinationFile.getName))

      try {
        val markdown = streamToString(new FileInputStream(sourceFile))
        val html = new MarkdownProcessor().markdown(markdown)
        val writer = new FileWriter(destinationFile)
        writer.write(template.replace("{{content}}", html))
        writer.close()
      } catch { case e =>
        usage(e)
        ""
      }

    println("done")
  }
}
