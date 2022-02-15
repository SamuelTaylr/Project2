import org.apache.spark.sql.SparkSession

class menu {

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

      }
      case 2 => {

      }
      case 3 => {

      }
      case 4 => {

      }
      case 5 => {
        System.exit(0)
      }
    }
  }
}