package jp.co.bizreach.dynamodb4s

import scala.annotation.StaticAnnotation

final case class hashPk() extends StaticAnnotation
final case class rangePk() extends StaticAnnotation
final case class name(name: String) extends StaticAnnotation
final case class version() extends StaticAnnotation
