package toast.config

import pureconfig.ConfigSource
import pureconfig.generic.auto._

final case class Config(
    generateEvents: Int,
    maxParams: Int,
    enableJobCreated: Boolean,
    enableJobInProgress: Boolean,
    enableJobSuccess: Boolean,
    enableJobFailed: Boolean
) {
  require(
    enableJobCreated || enableJobInProgress || enableJobSuccess || enableJobFailed,
    "at least one of enableJobCreated/enableJobInProgress/enableJobSuccess/enableJobFailed must be true"
  )
}

object Config {
  def load(): Config = ConfigSource.default.at("toast").loadOrThrow[Config]
}
