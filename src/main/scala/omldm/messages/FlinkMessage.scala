package omldm.messages

import java.io.Serializable
import BipartiteTopologyAPI.operations.RemoteCallIdentifier
import BipartiteTopologyAPI.sites.NodeId
import ControlAPI.{CountableSerial, Request}

abstract class FlinkMessage extends CountableSerial {
  var networkId: Int
  var operation: RemoteCallIdentifier
  var source: NodeId
  var destination: NodeId
  var data: Serializable
  var request: Request

  override def getSize: Int = {
    4 +
      { if (operation != null) operation.getSize else 0 } +
      { if (source != null) source.getSize else 0 } +
      { if (destination != null) destination.getSize else 0 } +
      { if (data != null && data.isInstanceOf[CountableSerial]) data.asInstanceOf[CountableSerial].getSize else 0 } +
      { if (request != null) request.getSize else 0 }
  }

}
