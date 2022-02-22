package group
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}

object loginMethods {

  def login(username: String, password: String): Boolean = {
    val db = new dbGetConnection
    val connection = db.dbConnection()
    var username2 = ""
    var password2 = ""
    var bool = false

    try {

      val statement = connection.createStatement()
      var cmd= s"Select * from login where username = '$username' and password = '$password'"
      val resultSet = statement.executeQuery(cmd)

      while (resultSet.next()) {
        username2 = resultSet.getString("username")
        password2 = resultSet.getString("password")

      }

      if(username.equals(username2)&&(password.equals(password2))) {
        bool = true

      }
      else{ bool = false}
    }
    catch {
      case e => e.printStackTrace
    }
    return bool

  }

  def addNewUser(username: String, password: String) : Boolean = {
    val db = new dbGetConnection
    val connection = db.dbConnection()
    val rs = "insert into login (username, password) values (?,?)"

    try {
      val preparedStmt: PreparedStatement = connection.prepareStatement(rs)
      preparedStmt.setString(1, username)
      preparedStmt.setString(2,password)
      preparedStmt.execute()
    }
    catch {
      case e => e.printStackTrace
    }
    return true
  }


}
