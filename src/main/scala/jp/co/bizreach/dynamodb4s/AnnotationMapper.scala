package jp.co.bizreach.dynamodb4s

import reflect.ClassTag
import reflect.runtime._
import universe._

class AnnotationMapper[T: TypeTag: ClassTag] {
  /** Annotated ClassSymbol */
  lazy val classSymbol: Symbol = typeOf[T].typeSymbol.asClass

  /** Annotated class member Symbol */
  lazy val memberSymbol: Seq[Symbol] = typeOf[T].member(termNames.CONSTRUCTOR).typeSignature match {
    // only take annotated members
    case MethodType(params, _) => params.filter(_.annotations.size > 0)
  }

  /**
   * Returns the Symbol specified the annotation type.
   * @param symbols
   * @tparam A annotation class
   * @return
   */
  def getAs[A: TypeTag](symbols: Symbol*): Option[Symbol] = symbols.find {
    _.annotations.find(_.tree.tpe <:< typeOf[A]).isDefined
  }

  /**
   * Returns the value stored in the field.
   * @param instance
   * @param m
   * @return
   */
  def getValue(instance: T, m: Symbol): Any = {
    currentMirror
      .reflect(instance)
      .reflectField(typeOf[T].member(m.name).asTerm)
      .get
  }



}
