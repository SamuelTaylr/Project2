package group

import org.apache.spark.sql.SparkSession

import java.io.FileInputStream
import scalafx.application.JFXApp
import scalafx.geometry.Insets
import scalafx.scene.Scene
import scalafx.scene.effect.DropShadow
import scalafx.scene.layout.HBox
import scalafx.scene.paint.Color._
import scalafx.scene.paint._
import scalafx.scene.text.Text
import scalafx.application.JFXApp
import scalafx.geometry.Insets
import scalafx.scene.Scene
import scalafx.scene.effect.DropShadow
import scalafx.scene.layout.HBox
import scalafx.scene.paint.Color._
import scalafx.scene.paint._
import scalafx.scene.text.Text
import scalafx._
import scalafx.event.ActionEvent
import scalafx.scene.control.{Alert, Button, Label, PasswordField, TextField}
import scalafx.Includes._
import scalafx.scene.control.Alert.AlertType
import scalafx.scene.image.{Image, ImageView}

object testObject extends JFXApp {

  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("Spark Works Y'all")
  spark.sparkContext.setLogLevel("ERROR")



  menuStage()

  def menuStage(): Unit = {

    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(300, 100) {
        val label = new Label("Enter Username and Password")
        label.layoutX = 20
        label.layoutY = 10
        val button = new Button("Login")

        button.onAction = (e: ActionEvent) => {
          val user = userField.text.value
          val password = passField.text.value
          var bool = false
          bool = loginMethods.login(user,password)

          if ((bool == true) && (user == "admin")) {
            adminStage()

            /*if(user == "admin") {
              adminStage()
            }
            else {
              menuStageTwo()
            }*/
          } else if ((bool == true) && (user.length > 1)) {
            menuStageTwo()
          }
          else {
            new Alert(AlertType.Information) {
              initOwner(stage)
              title = "Warning"
              contentText = "Incorrect Username or Password"
            }.showAndWait()
            menuStage()
          }
        }

        button.layoutX = 200
        button.layoutY = 50
        val userField = new TextField()
        userField.layoutX = 20
        userField.layoutY = 30
        val passField = new PasswordField()
        passField.layoutX = 20
        passField.layoutY = 60

        content = List(label, button, userField, passField)


      }

    }
  }
  def menuStageTwo(): Unit = {

    val sam = new sam
    val jake = new jake

    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(300, 200) {

        val label = new Label("Project 2 Queries:")
        label.layoutX = 20
        label.layoutY = 10

        val button = new Button("Query 1")
        button.onAction = (e: ActionEvent) => {
          //sam.dataLoader(spark, dataFrameCreator.dataLoader(spark))
          submenuQuery1()
        }
        button.layoutX = 20
        button.layoutY = 30

        val button2 = new Button("Query 2")
        button2.onAction = (e: ActionEvent) => {
          //sam.dataLoader(spark, dataFrameCreator.dataLoader(spark))
          sam.query2(spark)
          worldDeaths()
        }
        button2.layoutX = 20
        button2.layoutY = 70

        val button3 = new Button("Query 3")
        button3.onAction = (e: ActionEvent) => {
          jake.rollingMonthAnalysis(spark)
          query3Jacob()
        }
        button3.layoutX = 20
        button3.layoutY = 110

        val button4 = new Button("Query 4")
        button4.onAction = (e: ActionEvent) => {
          query4Jacob()
        }
        button4.layoutX = 20
        button4.layoutY = 150

        val button5 = new Button("Query 5")
        button5.onAction = (e: ActionEvent) => {
          mark.queryToDataSet(mark.avgCasesByMonth(spark),spark)
          query5Mark()
        }
        button5.layoutX = 100
        button5.layoutY = 30

        val button6 = new Button("Query 6")
        button6.onAction = (e: ActionEvent) => {
          mark.query2ToDataSet(mark.avgCasesByQuarter(spark),spark)
          query6Mark()
        }
        button6.layoutX = 100
        button6.layoutY = 70

        val button7 = new Button("Query 7")
        button7.onAction = (e: ActionEvent) => {
          mandeep.Query3(spark)
        }
        button7.layoutX = 100
        button7.layoutY = 110

        val button8 = new Button("Query 8")
        button8.onAction = (e: ActionEvent) => {
          mandeep.Query4(spark)
        }
        button8.layoutX = 100
        button8.layoutY = 150

        val button9 = new Button("ML Scenario")
        button9.onAction = (e: ActionEvent) => {
          //mlQuery()
          mlMethod.mlFunction()
          mlQuery()
        }
        button9.layoutX = 180
        button9.layoutY = 80


        content = List(label, button, button2, button3, button4, button5, button6, button7, button8, button9 )


      }
      stage.centerOnScreen()

    }



  }

  def adminStage(): Unit = {

    stage = new JFXApp.PrimaryStage {
      title = "Admin Panel"
      scene = new Scene(300, 100) {
        val label = new Label("Add a new user: ")
        label.layoutX = 20
        label.layoutY = 10
        val button = new Button("Confirm")

        button.onAction = (e: ActionEvent) => {
          val user = userField.text.value
          val password = passField.text.value
          loginMethods.addNewUser(user,password)
          new Alert(AlertType.Information) {
            initOwner(stage)
            title = "Info"
            contentText = "New User Added Successfully"
          }.showAndWait()
          menuStage()


        }

        button.layoutX = 200
        button.layoutY = 50
        val userField = new TextField()
        userField.layoutX = 20
        userField.layoutY = 30
        val passField = new PasswordField()
        passField.layoutX = 20
        passField.layoutY = 60

        content = List(label, button, userField, passField)


      }

    }
  }

  def submenuQuery1() : Unit = {
    val sam = new sam

    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(300, 200) {

        val label = new Label("Query 1 - Subqueries:")
        label.layoutX = 20
        label.layoutY = 10

        val button = new Button("US Conf. Cases")
        button.onAction = (e: ActionEvent) => {
          sam.subQuery1(spark)
          usConfCases()
        }
        button.layoutX = 20
        button.layoutY = 30

        val button2 = new Button("India Conf. Cases")
        button2.onAction = (e: ActionEvent) => {
          sam.subQuery2(spark)
          indiaConfCases()

        }
        button2.layoutX = 20
        button2.layoutY = 70

        val button3 = new Button("China Conf. Cases")
        button3.onAction = (e: ActionEvent) => {
          sam.subQuery3(spark)
          chinaConfCases()
        }
        button3.layoutX = 20
        button3.layoutY = 110

        val button4 = new Button("US Deaths")
        button4.onAction = (e: ActionEvent) => {
          sam.subQuery4(spark)
          usDeaths()

        }
        button4.layoutX = 140
        button4.layoutY = 30

        val button5 = new Button("India Deaths")
        button5.onAction = (e: ActionEvent) => {
          sam.subQuery5(spark)
          indiaDeaths()
        }
        button5.layoutX = 140
        button5.layoutY = 70

        val button6 = new Button("China Deaths")
        button6.onAction = (e: ActionEvent) => {
          sam.subQuery6(spark)
          chinaDeaths()
        }
        button6.layoutX = 140
        button6.layoutY = 110

        val button7 = new Button("Back")
        button7.onAction = (e: ActionEvent) => {
          menuStageTwo()
        }
        button7.layoutX = 140
        button7.layoutY = 150

        content = List(label, button, button2, button3, button4, button5, button6, button7 )


      }
    }

  }

  def usConfCases(): Unit = {

    val stream = new FileInputStream("input/USConfCases.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 800) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 20
        button.onAction = (e: ActionEvent) => {
          submenuQuery1()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def indiaConfCases(): Unit = {

    val stream = new FileInputStream("input/IndiaConfCases.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 800) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 20
        button.onAction = (e: ActionEvent) => {
          submenuQuery1()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def chinaConfCases(): Unit = {

    val stream = new FileInputStream("input/ChinaConfCases.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 800) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 20
        button.onAction = (e: ActionEvent) => {
          submenuQuery1()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def usDeaths(): Unit = {

    val stream = new FileInputStream("input/USDeaths.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 800) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 20
        button.onAction = (e: ActionEvent) => {
          submenuQuery1()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def indiaDeaths(): Unit = {

    val stream = new FileInputStream("input/IndiaDeaths.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 800) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 20
        button.onAction = (e: ActionEvent) => {
          submenuQuery1()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def chinaDeaths(): Unit = {

    val stream = new FileInputStream("input/ChinaDeaths.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 800) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 20
        button.onAction = (e: ActionEvent) => {
          submenuQuery1()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def worldDeaths(): Unit = {

    val stream = new FileInputStream("input/WorldDeaths.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 800) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 20
        button.onAction = (e: ActionEvent) => {
          menuStageTwo()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def query3Jacob(): Unit = {

    val stream = new FileInputStream("input/Query3-1.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 900) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 870
        button.onAction = (e: ActionEvent) => {
          menuStageTwo()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def query4Jacob(): Unit = {

    val stream = new FileInputStream("input/Query4.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 900) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 870
        button.onAction = (e: ActionEvent) => {
          menuStageTwo()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def query5Mark(): Unit = {

    val stream = new FileInputStream("input/Query5.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 850) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 20
        button.onAction = (e: ActionEvent) => {
          menuStageTwo()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def query6Mark(): Unit = {

    val stream = new FileInputStream("input/Query6.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 850) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 20
        button.onAction = (e: ActionEvent) => {
          menuStageTwo()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def query7Mandeep(): Unit = {

    val stream = new FileInputStream("input/Query5.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 850) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 20
        button.onAction = (e: ActionEvent) => {
          menuStageTwo()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def query8Mandeep(): Unit = {

    val stream = new FileInputStream("input/Query5.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 850) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 20
        button.onAction = (e: ActionEvent) => {
          menuStageTwo()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

  def mlQuery(): Unit = {

    val stream = new FileInputStream("input/ML.png")
    val image = new Image(stream)
    val imageview = new ImageView()
    imageview.setImage(image)
    imageview.preserveRatio


    stage = new JFXApp.PrimaryStage {
      title = "Project 2"
      scene = new Scene(1250, 850) {

        val button = new Button("Back")
        button.layoutX = 10
        button.layoutY = 20
        button.onAction = (e: ActionEvent) => {
          menuStageTwo()
        }

        content = List(imageview, button)
      }

    }
    stage.centerOnScreen()
  }

}
