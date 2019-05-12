package prv.saevel.streaming.comparison.common.tests.utils

import java.net.ServerSocket

import scala.util.{Failure, Success, Try}

trait NetworkUtils {

  def randomAvailablePort: Int = Try(new ServerSocket(0)) match {
    case Success(socket) => socket.getLocalPort
    case Failure(e) => throw e
  }
}
