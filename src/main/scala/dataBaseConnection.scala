import ScalaJdbcConnectSelect.{connection, resultSet}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object dataBaseConnection {
  var connection: Connection=null
  val url = "jdbc:mysql://localhost:3306/project2"
  val username = "root"
  val password = "matwal"

 /* var connection = Connection
  var connection:Connection = null
  var resultSet: ResultSet = null
  var myStmt: PreparedStatement=null*/

  def dbConnection():Connection= {

    try {
      //Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      println("connection establish")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
     return connection
  }



  def main(args: Array[String]): Unit = {

    /*val menu = new menu
    menu.selectionMenu(spark)*/

    /*val sam = new sam
    sam.LogIn(connection)*/
    //val mandeep =new mandeep
    //mandeep.logIn(connection)

  }

}
