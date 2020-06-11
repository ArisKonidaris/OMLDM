package omldm.nodes.hub

import org.apache.flink.api.common.functions.Function

/**
  * The basic trait of a coordinator for the distributed star topology.
  */
trait Hub extends Function with Serializable