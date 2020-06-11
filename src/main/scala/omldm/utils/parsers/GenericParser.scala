package omldm.utils.parsers

trait GenericParser[T, U] {
  def parse(input: T): U
}
