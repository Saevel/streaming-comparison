package prv.saevel.streaming.comparison.common.tests.utils

trait StreamingTest[Serializer[_], Deserializer[_], X, Y] extends StreamWriter[Serializer, X] with StreamReader[Deserializer, Y]