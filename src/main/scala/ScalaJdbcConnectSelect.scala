import Package2.dataBaseConnection
import sun.security.util.Password

import java.lang.System.console
import java.security.MessageDigest
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import scala.io.StdIn._

object ScalaJdbcConnectSelect {
  // connect to the database named "mysql" on the localhost
  val con = dataBaseConnection
  val connection=con.dbConnection()
  var resultSet: ResultSet = null
  var myStmt: PreparedStatement=null


  def executeQuery(cmd: String): ResultSet ={
    var statement = connection.createStatement()
    resultSet = statement.executeQuery(cmd)
    resultSet
  }


  // EncryptedPassword method
  /*def getEncryptedPassword(password: String): String = {
    MessageDigest
      .getInstance("SHA-256")
      .digest(password.getBytes("UTF-8"))
      .map("%02X".format(_))
      .mkString
  }*/
  def mask(password:String): Unit ={
    print(mask(password))
  }

  /*def pollForInput(query: String = ">Enter your Input"): String = {
    val input = readLine(s"$query\n")
    input
  }*/

  def login() {
    val connection=con.dbConnection()
    println("connection created")
    val name=readLine("Username:")
    val pass = readLine("PasswordName:")
   //val pass = console().readPassword("PasswordName:")



    //val pass = readLine("PasswordName: ")
    println("username "+name,"password "+pass)
    try {
      // create the statement, and run the select query
      val statement = connection.createStatement()
      //var cmd="SELECT FROM LogIn WHERE UserName = ? AND PasswordName = ?"
      var cmd = "Select UserName,PasswordName from LogIn";
      println(cmd)
      //dbConnection()
      resultSet = executeQuery(cmd)
      println("resultset value" + resultSet)
      while (resultSet.next()) {
        val UserName = resultSet.getString("UserName")
        val PasswordName = resultSet.getString("PasswordName")
        if (UserName.equals(name) && (PasswordName.equals(pass))) {
          println("you are login")
        }
      }
    }
  catch {
    case e => e.printStackTrace
  }
}
    /*Database.GetUser(name) match {
      case Some(res) => {
        if (!(res.GetPassword() == pass && res.GetUsername() == name)) { println("Incorrect login\n"); return }

        State.SetLoggedIn(true)
        State.SetUser(res)
      }*/
     /* case None => {
        println(s"Incorrect login\n")
      }*/


  /*def login(): Unit= {
   try {
     // create the statement, and run the select query
     val statement = connection.createStatement()
     //var cmd="SELECT FROM LogIn WHERE UserName = ? AND PasswordName = ?"
     var cmd="Select UserName,PasswordName from LogIn";
     //dbConnection()
     resultSet=executeQuery(cmd)
    //val resultSet =statement.executeQuery("SELECT FROM LogIn WHERE UserName = ? AND PasswordName = ?")
     //val resultSet = statement.executeQuery("Select UserName,PasswordName from LogIn")
     while (resultSet.next()) {
       val UserName = resultSet.getString("UserName")
       val PasswordName = resultSet.getString("PasswordName")
      // val PasswordName = getEncryptedPassword(PasswordName)
     //  println("UserName, Password = " + UserName + ", " + PasswordName)

       println("Enter Usename")
       var User_Name = scala.io.StdIn.readLine()
       println("Enter Password")
       var Pass_word1 = scala.io.StdIn.readLine()
       //val Pass_word  = getEncryptedPassword(Pass_word1 )
       if(UserName.equals(User_Name)&&(PasswordName.equals(Pass_word))) {
         println("you are login")
       }
       else if(!(UserName.equals(User_Name)&&(PasswordName.equals(Pass_word)))){
          println("Register Input")
          //registerUser()
        }
       else{ println("wrong input")}

     }
   }
   catch {
     case e => e.printStackTrace
   }
  }*/

  /*def registerUser(): Unit= {
    println("register here")
    try {
      println("Enter usename")
      var UserName = scala.io.StdIn.readLine()
      println("Enter password")
      var PasswordName = scala.io.StdIn.readLine()

      var cmd = "INSERT INTO LogIn(UserName,PasswordName)VALUES(?,?)";
      println(cmd);
      //var db = dbConnection()
      myStmt = connection.prepareStatement(cmd);
      myStmt.setString(1,UserName )
      myStmt.setString(2,PasswordName)
      //myStmt.executeUpdate(cmd)
      myStmt.execute()
      println("now you are Registered")
     // login()
    }
    catch {
      case e => e.printStackTrace
    }
  }*/
  def exit():Unit={
    println("exit")

  }

  /*def loginScreen(): Boolean = {
    println("\nWelcome to Covid Slots. Please log in or create a new user if you're new")
    println("[1]: Log In")
    println("[2]: Register New User")
    //println("[3]: Find User")
    println("[4]: Exit")
    val input = readInt().toString
    input match {
      case "1" => { login() }
     // case "2" => { registerUser() }
      //case "3" => { findUser() }
      case "4" => { exit() }
      case _ => println("Invalid choice\n")
    }

    return false
  }*/



  def main(args: Array[String]): Unit = {
    //dbConnection()
    login();
    //loginScreen()

}

}




