package omldm.utils

object DefaultJobParameters {
  val defaultJobName: String = "OML_job_1"
  val defaultParallelism: String = "16"
  val defaultMaxMsgParams: String = "2000"
  val defaultInputFile: String = "hdfs://clu01.softnet.tuc.gr:8020/user/vkonidaris/lin_class_mil_e10.txt"
  val defaultOutputFile: String = "hdfs://clu01.softnet.tuc.gr:8020/user/vkonidaris/output"
  val defaultTestParameter: String = "true"
  val defaultTimeout: String = "30000"
  val defaultTestSetSize: String = "256"
}
