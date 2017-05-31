package bytetrend

import java.io.File

import bytetrend.avro.User
import org.apache.avro.Schema


/**
  * https://avro.apache.org/docs/1.8.1/gettingstartedjava.html
  * Run from command line:
  * mvn -q exec:java -Dexec.mainClass=bytetrend.GenericAvro
  *
  * @author ${user.name}.
  */
object GenericAvro {

  def schema = new Schema.Parser().parse(new File("src/main/avro/User.avsc"))

  def serialize = {
    val schm = schema
    import org.apache.avro.generic.GenericData
    val user1 = new GenericData.Record(schm)
    user1.put("name", "Alyssa")
    user1.put("favorite_number", 256)
    // Leave favorite color null

    val user2 = new GenericData.Record(schm)
    user2.put("name", "Ben")
    user2.put("favorite_number", 7)
    user2.put("favorite_color", "red")

    import org.apache.avro.file.DataFileWriter
    import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
    // Serialize user1 and user2 to disk// Serialize user1 and user2 to disk

    val file = new File("users.avro")
    val datumWriter = new GenericDatumWriter[GenericRecord](schm)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(schm, file)
    dataFileWriter.append(user1)
    dataFileWriter.append(user2)
    dataFileWriter.close()
  }

  def deserialize = {
    import org.apache.avro.file.DataFileReader
    import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
    import scala.collection.JavaConversions._
    // Deserialize users from disk// Deserialize users from disk

    val datumReader = new GenericDatumReader[GenericRecord](schema)
    val dataFileReader = new DataFileReader[GenericRecord](new File("users.avro"), datumReader)

    for(
      user <- dataFileReader.iterator()
    ) println(user)
  }

  def main(args: Array[String]) {
    serialize
    deserialize
  }
}
