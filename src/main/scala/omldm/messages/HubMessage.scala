package omldm.messages

import java.io.Serializable
import BipartiteTopologyAPI.operations.RemoteCallIdentifier
import BipartiteTopologyAPI.sites.NodeId
import ControlAPI.{CountableSerial, Request}

case class HubMessage(var networkId: Int,
                      var operations: Array[RemoteCallIdentifier],
                      var source: NodeId,
                      var destinations: Array[NodeId],
                      var data: Serializable,
                      var request: Request) extends Serializable {

  def this() = this(
    -1,
    new Array[RemoteCallIdentifier](1),
    new NodeId(null, 0),
    new Array[NodeId](1),
    null,
    null
  )

  def getNetworkId: Int = networkId

  def setNetworkId(networkId: Int): Unit = this.networkId = networkId

  def getOperations: Array[RemoteCallIdentifier] = operations

  def setOperations(operations: Array[RemoteCallIdentifier]): Unit = this.operations = operations

  def getSource: NodeId = source

  def setSource(source: NodeId): Unit = this.source = source

  def getDestinations: Array[NodeId] = destinations

  def setDestinations(destinations: Array[NodeId]): Unit = this.destinations = destinations

  def getData: Serializable = data

  def setData(data: CountableSerial): Unit = this.data = data

  def getRequest: Request = request

  def setRequest(request: Request): Unit = this.request = request

  def getSize: Int = {
    4 +
      { if (operations != null) operations.length * (for (rci <- operations) yield rci.getSize).sum else 0 } +
      { if (source != null) source.getSize else 0 } +
      { if (destinations != null) destinations.length * (for (rci <- destinations) yield rci.getSize).sum else 0 } +
      { if (data != null && data.isInstanceOf[CountableSerial]) data.asInstanceOf[CountableSerial].getSize else 0 } +
      { if (request != null) request.getSize else 0 }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case HubMessage(net, ops, src, dsts, dt, req) =>
        networkId == net &&
          operations.equals(ops) &&
          source == src &&
          destinations .equals(dsts) &&
          data.equals(dt) &&
          request.equals(req)
      case _ => false
    }
  }

  override def toString: String = s"HubMessage(" + s"$networkId, ${operations.mkString("Array(", ", ", ")")}, $source, ${destinations.mkString("Array(", ", ", ")")}, $data, $request)"

}
