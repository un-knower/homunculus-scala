package com.homunculus.basis

object InnerClassDriver extends App{
  //调用外部类中的内部类对象
  println(new OuterClass().InnerObject.y)

  //调用外部类中的内部类
  val oc = new OuterClass
  var ic = new oc.InnerClass
  println(ic.x)

  //调用外部对象的内部类
  println((new OuterObject.InnerClass).m)

  //调用外部对象的内部对象
  println(OuterObject.InnerObject.n)

}

class OuterClass{
  class InnerClass{
    var x = 1
  }
  object InnerObject{
    val y = 2
  }
}
object OuterObject{
  class InnerClass{
    var m = 3
  }

  object InnerObject{
    var n = 4
  }
}
