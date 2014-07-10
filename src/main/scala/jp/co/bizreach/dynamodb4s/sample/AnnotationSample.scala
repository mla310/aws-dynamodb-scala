package jp.co.bizreach.dynamodb4s.sample

import annotation.StaticAnnotation
import jp.co.bizreach.dynamodb4s.AnnotationMapper

/**
 * Sample code for annotation information.
 */
object AnnotationSample {
  def main(args: Array[String]): Unit = {

    val instance = SampleEntity(10, "hoge", 1)
    val mapper = new AnnotationMapper[SampleEntity]

    val id = mapper.getAs[id]
    println("***: " + id.map(_.name))

    val value = mapper.getValue(instance, id.get)
    println("***: " + value)

    val annotation = mapper.annotation[db](mapper.classSymbol).get
    val attr = mapper.annotationAttribute(annotation)
    println("---: " + attr)
  }

}

final case class db(name: String) extends StaticAnnotation
final case class id() extends StaticAnnotation
//final case class version(arg1: String, arg2: String) extends StaticAnnotation

@db("SampleEntity")
case class SampleEntity(
  @id
  userId: Long,
  userName: String,
  //@version("arg1", "version number")
  versionNo: Long
)
