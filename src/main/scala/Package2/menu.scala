package Package2

import org.apache.spark.sql.SparkSession

class menu extends mandeep{
 //var result=new mandeep()
  def selectionMenu(spark: SparkSession) : Unit = {
    val mandeep =new mandeep()

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
        mandeep.dataLoader(spark)
        mandeep.Query1(spark)
        selectionMenu(spark)
      }
      case 2 => {
        mandeep.dataLoader(spark)
        mandeep.Query2(spark)
        selectionMenu(spark)
      }
      case 3 => {
        mandeep.dataLoader(spark)
        mandeep.Query3(spark)
        selectionMenu(spark)
      }
      case 4 => {
        mandeep.dataLoader(spark)
        mandeep.Query4(spark)
        selectionMenu(spark)
      }
      case 5 => {

        sys.exit(0)
      }
    }
  }
}