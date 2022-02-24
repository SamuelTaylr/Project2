package group

import java.sql.{Connection, DriverManager}

// sandbox to create database
class dbGetConnection {
  def dbConnection(): Connection = {
    val mySQLURL = "jdbc:mysql://useraccess.cwoofs136vmw.us-west-1.rds.amazonaws.com/useraccess"
    val databaseUserName = "admin"
    val databasePassword = "adminHhg5"
    val driver = "com.mysql.cj.jdbc.Driver"
    var con = DriverManager.getConnection(mySQLURL, databaseUserName, databasePassword)
    try{
      Class.forName(driver)
      con = DriverManager.getConnection(mySQLURL, databaseUserName, databasePassword)
    } catch {
      case e: Exception =>
        e.printStackTrace()
      case f: Error =>
        f.printStackTrace()
    }
    return con
  }
}