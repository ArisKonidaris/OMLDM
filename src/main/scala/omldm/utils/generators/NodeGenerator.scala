package omldm.utils.generators

import BipartiteTopologyAPI.NodeInstance
import ControlAPI.Request

trait NodeGenerator extends java.io.Serializable {
  def setMaxMsgParams(maxMsgParams: Int): NodeGenerator
  def generateSpokeNode(request: Request): NodeInstance[_,_]
  def generateHubNode(request: Request): NodeInstance[_,_]
}
