package Package2

import org.apache.spark.sql.SparkSession

class menu {
 //var sam=new sam()
  def selectionMenu(spark: SparkSession) : Unit = {

    println(
      """
        |************************
        |Enter a number to select
        |a menu item.************
        |************************
        |1. Query 1 *************
        |2. Query 2 *************
        |3. Query 3 *************
        |4. Query 4 *************
        |5. Quit ****************
        |************************
        |""".stripMargin)
    val selection = readInt()

    selection match {
      case 1 => {
       //QUERY1()
        selectionMenu(spark)
      }
      case 2 => {
       // sam.QUERY2()
        selectionMenu(spark)
      }
      case 3 => {
      //  sam.QUERY3()
        selectionMenu(spark)
      }
      case 4 => {
       // sam.QUERY4()
        selectionMenu(spark)
      }
      case 5 => {
      //  sam.QUERY5()
        sys.exit(0)
      }
    }
  }
}