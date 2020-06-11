package omldm.messages

import java.io.Serializable

import BipartiteTopologyAPI.operations.RemoteCallIdentifier
import BipartiteTopologyAPI.sites.NodeId
import ControlAPI.Request

abstract class FlinkMessage extends Serializable {
  var networkId: Int
  var operation: RemoteCallIdentifier
  var source: NodeId
  var destination: NodeId
  var data: Serializable
  var request: Request
}
