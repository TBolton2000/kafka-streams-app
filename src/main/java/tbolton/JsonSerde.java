// import org.apache.kafka.common.serialization.Deserializer;
// import org.apache.kafka.common.serialization.Serde;
// import org.apache.kafka.common.serialization.Serdes;
// import org.apache.kafka.common.serialization.Serializer;

// public class KafkaJsonSerdes<T> implements {

//     public class KafkaJsonSerializer implements Serializer {

//         private Logger logger = LogManager.getLogger(this.getClass());

//         @Override
//         public void configure(Map map, boolean b) {

//         }

//         @Override
//         public byte[] serialize(String s, Object o) {
//             byte[] retVal = null;
//             ObjectMapper objectMapper = new ObjectMapper();
//             try {
//                 retVal = objectMapper.writeValueAsBytes(o);
//             } catch (Exception e) {
//                 logger.error(e.getMessage());
//             }
//             return retVal;
//         }

//         @Override
//         public void close() {

//         }
//     }

//     public class KafkaJsonDeserializer<T> implements Deserializer {

//         private Logger logger = LogManager.getLogger(this.getClass());

//         private Class <T> type;

//         public KafkaJsonDeserializer(Class type) {
//             this.type = type;
//         }

//         @Override
//         public void configure(Map map, boolean b) {

//         }

//         @Override
//         public Object deserialize(String s, byte[] bytes) {
//             ObjectMapper mapper = new ObjectMapper();
//             T obj = null;
//             try {
//                 obj = mapper.readValue(bytes, type);
//             } catch (Exception e) {

//                 logger.error(e.getMessage());
//             }
//             return obj;
//         }

//         @Override
//         public void close() {

//         }
//     }

// }