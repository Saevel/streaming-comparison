package prv.saevel.streaming.comparison.common.tests.scenarios
import org.scalacheck.Gen

trait BasicGenerators {

  protected val streetNames: Gen[String] = Gen.oneOf("Baker Street", "King's Street", "Main Street",
    "Liverpool Street", "Stone Street", "High Street", "King Edward Street", "Ring Road", "Castle Road", "Rose Avenue")

  protected val countries: Gen[String] = Gen.oneOf("United Kingdom", "United States", "Malaysia",
    "Canada", "Australia", "Ireland")

  protected def cities(country: String): Gen[String] = country match {
    case "United Kingdom" => Gen.oneOf("London", "Manchester", "Edinburgh", "Liverpool", "Bristol", "Southampton", "Cardiff")
    case "United States" => Gen.oneOf("New York", "Washington", "Huston", "Los Angeles", "Miami", "Chicago")
    case "Malaysia" => Gen.oneOf("Kuala Lumpur", "Butterworth", "Penang", "Kuching", "Kota Kinabalu")
    case "Canada" => Gen.oneOf("Toronto", "Vancouver", "Canberra")
    case "Australia" => Gen.oneOf("Sydney", "Melbourne", "Adelaide", "Darwin", "Perth", "Brisbane")
    case "Ireland" => Gen.oneOf("Cork", "Dublin", "Munster", "Derry")
  }

  protected def addresses(country: String): Gen[String] = for {
    streetName <- streetNames
    streetNo <- Gen.choose(1, 150)
    city <- cities(country)
  } yield s"$streetNo, $streetName, $city, $country"

  protected val names: Gen[String] = Gen.oneOf(
    "Liam", "Emma",
    "Noah", "Olivia",
    "William", "Ava",
    "James", "Isabella",
    "Logan", "Sophia",
    "Benjamin", "Mia",
    "Mason", "Charlotte",
    "Elijah", "Amelia",
    "Oliver", "Evelyn",
    "Jacob", "Abigail"
  )
  protected val surnames: Gen[String] = Gen.oneOf("Smith", "Johnson", "Williams", "Jones", "Brown",
    "Davis", "Miller", "Wilson", "Moore", "Taylor"
  )

  protected val smallInts = Gen.choose(1, 10)

  protected val mediumInts = Gen.choose(10, 100)
}
