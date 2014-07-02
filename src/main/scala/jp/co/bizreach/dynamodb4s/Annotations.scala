package jp.co.bizreach.dynamodb4s

import scala.annotation.StaticAnnotation

final case class DynamoTable(name: String) extends StaticAnnotation
final case class HashPk() extends StaticAnnotation
final case class RangePk() extends StaticAnnotation
