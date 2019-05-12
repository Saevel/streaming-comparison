package prv.saevel.streaming.comparison.common.utils

trait ContextProvider[ConfigType, ContextType] {

  def withContext[T](config: ConfigType)(f: ContextType => T): T
}
