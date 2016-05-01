package jp.co.bizreach.dynamodb4s

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanRequest, Select}
import jp.co.bizreach.dynamodb4s.DynamoTable._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

case class DynamoScanBuilder[T <: DynamoTable](
  private val _table: T,
  private val _attributes: T => Seq[DynamoProperty[_]],
  private val _filterExpression: Option[String] = None,
  private val _valueMap: Map[String, Any] = Map.empty,
  private val _limit: Int = 1000){

  def attributes(f: T => Seq[DynamoProperty[_]]): DynamoScanBuilder[T] = this.copy(_attributes = t => (_attributes(t) ++ f(t)))

  def attribute(f: T => DynamoProperty[_]): DynamoScanBuilder[T] = this.copy(_attributes = t => (_attributes(t) ++ Seq(f(t))))

  def limit(i: Int): DynamoScanBuilder[T] = this.copy(_limit = i)

  def filterExpression(expression: String, values: (String, Any)*): DynamoScanBuilder[T] =
    this.copy(_filterExpression = Some(expression), _valueMap = _valueMap ++ values)

  def foreach[E](mapper: (T, DynamoRow) => E)(implicit db: awscala.dynamodbv2.DynamoDB): Unit = {
    var lastKeyEvaluated: java.util.Map[String, AttributeValue] = null

    do {
      val result = db.scan(createRequest(_attributes(_table).map(_.name), lastKeyEvaluated))
      val items  = result.getItems
      items.asScala.foreach { item =>
        mapper(_table, new DynamoRow(item))
      }

      lastKeyEvaluated = result.getLastEvaluatedKey

    } while(lastKeyEvaluated != null)
  }

  def as[E <: AnyRef](f: E => Unit)(implicit db: awscala.dynamodbv2.DynamoDB, c: ClassTag[E]): Unit = {
    var lastKeyEvaluated: java.util.Map[String, AttributeValue] = null

    do {
      val result = db.scan(createRequest(c.runtimeClass.getDeclaredFields.map(_.getName), lastKeyEvaluated))
      val items  = result.getItems
      val tableInfo = getTableInfo(_table)
      val clazz  = c.runtimeClass
      val fields = clazz.getDeclaredFields

      items.asScala.map { x =>
        val c = clazz.getConstructors()(0)
        val args = c.getParameterTypes.map { x =>
          if(x == classOf[Int]) new Integer(0) else null
        }
        val o = c.newInstance(args: _*)
        fields.foreach { f =>
          f.setAccessible(true)
          val t = f.getType
          val attribute = x.get(f.getName)
          val property = tableInfo.getDynamoProperty(f.getName)
          if(t == classOf[Option[_]]){
            f.set(o, Option(getAttributeValue(attribute)).map(property.convert))
          } else {
            f.set(o, property.convert(getAttributeValue(attribute)))
          }
        }
        f(o.asInstanceOf[E])
      }

      lastKeyEvaluated = result.getLastEvaluatedKey

    } while(lastKeyEvaluated != null)
  }

  protected def createRequest(attributes: Seq[String], lastKeyEvaluated: java.util.Map[String, AttributeValue]): ScanRequest = {
    val req = new ScanRequest()
      .withTableName(_table.table)
      .withLimit(_limit)
      .withExclusiveStartKey(lastKeyEvaluated)

    _filterExpression match {
      case Some(expression) => {
        req.withFilterExpression(expression)
        if(_valueMap.nonEmpty){
          val valueMap = new java.util.HashMap[String, AttributeValue]()
          _valueMap.foreach { case (key, value) =>
            val attributeValue = new AttributeValue()
            value match {
              case x: String => attributeValue.setS(x)
              case x: Int => attributeValue.setN(x.toString)
            }
            valueMap.put(if(key.startsWith(":")) key else ":" + key, attributeValue)
          }
          req.withExpressionAttributeValues(valueMap)
        }
      }
      case None => {
        req.withSelect(Select.SPECIFIC_ATTRIBUTES)
        req.withAttributesToGet(attributes: _*)
      }
    }


    req
  }

}
