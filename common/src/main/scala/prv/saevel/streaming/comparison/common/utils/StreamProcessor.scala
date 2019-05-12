package prv.saevel.streaming.comparison.common.utils

trait StreamProcessor[Config, Context] {

  def startStream(config: Config)(implicit context: Context): Unit

  def stopStream(implicit context: Context): Unit
}
