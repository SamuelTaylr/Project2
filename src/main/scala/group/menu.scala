package group

import org.apache.spark.sql.SparkSession

class menu {

  def selectionMenu(spark: SparkSession) : Unit = {

    println(
      """
        |************************
        |Enter a number to select
        |a group.menu item.************
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


        selectionMenu(spark)
      }
      case 2 => {
        selectionMenu(spark)
      }
      case 3 => {
        selectionMenu(spark)
      }
      case 4 => {
        selectionMenu(spark)
      }
      case 5 => {
        sys.exit(0)
      }
    }
  }
}