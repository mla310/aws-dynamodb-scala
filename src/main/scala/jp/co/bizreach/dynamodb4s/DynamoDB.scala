package jp.co.bizreach.dynamodb4s

/**
 * Created by naoki.takezoe on 2014/07/02.
 */
trait DynamoDB {

  // TODO フィルタ更新
  def put(row: AnyRef): Unit
  def putAttributes(hashPk: Any)
  def putAttributes(hashPk: Any, rangePk: Any)

  def delete(hashPk: Any)
  def delete(hashPk: Any, rangePk: Any)

  def search()

}

//class DynamoDBImpl(implicit db: awscala.dynamodbv2.DynamoDB) extends DynamoDB {
//
//  // TODO フィルタ更新
//  def put(row: AnyRef): Unit
//  def putAttributes(hashPk: Any)
//  def putAttributes(hashPk: Any, rangePk: Any)
//
//  def delete(hashPk: Any) = db.deleteItem()
//  def delete(hashPk: Any, rangePk: Any)
//
//  def search()
//
//}