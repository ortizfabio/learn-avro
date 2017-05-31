package bytetrend

import java.io.File

import bytetrend.avro.User

/**
  * https://avro.apache.org/docs/1.8.1/gettingstartedjava.html
  * Run from command line:
  * mvn -q exec:java -Dexec.mainClass=bytetrend.AvroApp
  *
  * @author ${user.name}
  */
object AvroApp {

  def serialize()  = {
    val user1 = new User()
    user1.setName("Alyssa")
    user1.setFavoriteNumber(256)
    // Leave favorite color null

    // Alternate constructor
    val user2 = new User("Ben", 7, "red")
    val user3 =  User.newBuilder()
      .setName("Charlie")
      .setFavoriteColor("blue")
      .setFavoriteNumber(null)
      .build()
    import bytetrend.avro.User
    import org.apache.avro.file.DataFileWriter
    import org.apache.avro.io.DatumWriter
    import org.apache.avro.specific.SpecificDatumWriter
    // Serialize user1, user2 and user3 to disk// Serialize user1, user2 and user3 to disk

    val userDatumWriter = new SpecificDatumWriter[User](classOf[User])
    val dataFileWriter = new DataFileWriter[User](userDatumWriter)

    dataFileWriter.create(user1.getSchema, new File("users.avro"))
    dataFileWriter.append(user1)
    dataFileWriter.append(user2)
    dataFileWriter.append(user3)
    dataFileWriter.close
  }

  def deserialize(): Unit ={
    import scala.collection.JavaConversions._
    import bytetrend.avro.User
    import org.apache.avro.file.DataFileReader
    import org.apache.avro.io.DatumReader
    import org.apache.avro.specific.SpecificDatumReader
    val userDatumReader = new SpecificDatumReader[User](classOf[User])
    val dataFileReader = new DataFileReader[User](new File("users.avro"), userDatumReader)

    for(
      user <- dataFileReader.iterator()
    ) println(user)
  }

  def main(args: Array[String]) {
    serialize
    deserialize
  }

}
