package jp.co.bizreach.dynamodb4s

import annotation.StaticAnnotation

/**
 * Sample code for annotation information.
 */
object AnnotationSample {
  def main(args: Array[String]): Unit = {

    val instance = SampleEntity(10, "hoge", 1)
    val mapper = new AnnotationMapper[SampleEntity]

    val id = mapper.getAs[id](mapper.memberSymbol: _*)
    println("***: " + id.map(_.name))

    val value = mapper.getValue(instance, id.get)
    println("***: " + value)
  }

}

final case class db() extends StaticAnnotation
final case class id() extends StaticAnnotation
final case class version() extends StaticAnnotation

@db
case class SampleEntity(
  @id
  userId: Long,
  userName: String,
  @version
  versionNo: Long
)
