import com.twitter.logging.config._

new LoggerConfig {
  level = Level.INFO
  handlers = new FileHandlerConfig {
    filename = "/var/log/kestrel/kestrel.log"
    roll = Policy.Never
  }
}
