package omldm.job

/**
 * A basic trait for an OMLDM job.
 *
 * @tparam T The output stream(s) of the OMLDM job.
 */
trait FlinkJob[T] {

  def getJobOutput: T

}