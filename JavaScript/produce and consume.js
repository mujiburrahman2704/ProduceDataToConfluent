const { Kafka, Partitioners } = require("kafkajs")
const { Agent } = require ("https")
const fs = require("fs")
const { SchemaRegistry } = require("@kafkajs/confluent-schema-registry")

const agent = new Agent({ 
    keepAlive: true,
    ca: [fs.readFileSync('ca.crt', 'utf-8')],
    key: fs.readFileSync('kafka_broker.key', 'utf-8'),
    cert: fs.readFileSync('kafka_broker.crt', 'utf-8')
})

const registry = new SchemaRegistry({ 
    host: '', //Isi dengan host dari Schema registry
    auth: {
      username: '', //username dari user yang dapat digunakan untuk melakukan authentikasi ke schema registry
      password: '' //password dari user yang dapat digunakan untuk melakukan authentikasi ke schema registry
    },
    agent
})

const kafka = new Kafka({ 
    clientId: 'my-app',
    brokers: [], //isi dengan host cluster dari kafka broker
    ssl: {
      rejectUnauthorized: false,
      ca: [fs.readFileSync('ca.crt', 'utf-8')], //isi dengan sertifikat CA
      key: fs.readFileSync('kafka_broker.key', 'utf-8'), //isi dengan key dari sertifikat
      cert: fs.readFileSync('kafka_broker.crt', 'utf-8') //isi dengan sertifikat crt untuk koneksi ke kafka
    },
    sasl: {
      mechanism: '', //isi dengan mechanism yang digunakan untuk koneksi ke kafka
      username: 'c3', //isi dengan username yang dapat digunakan untuk terkoneksi ke kafka broker
      password: 'c31' //isi dengan password yang dapat digunakan untuk terkoneksi ke kafka broker
    }
})

const consumer = kafka.consumer({ groupId: 'test' }) // isi dengan nama group untuk consumer group
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })
const topic = "topic-test" // isi dengan topic
const run = async () => {
    const subject = 'topic-test-value'
    const id = await registry.getLatestSchemaId(subject)
    await consumer.connect()
    await producer.connect()
  
    await consumer.subscribe({ topic: topic , fromBeginning: true})
  
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const decodedMessage = {
          ...message,
          value: await registry.decode(message.value)
        }
        console.log("Success This is Data consume = ", decodedMessage.value)
  
        const outgoingMessage = {
          key: message.key,
          value: await registry.encode(id, decodedMessage.value)
        }
  
        await producer.send({
          topic: topic,
          messages: [ outgoingMessage ]
        })
        console.log("Success This is Data consume = ", decodedMessage.value)
      },
    })
  }
  
  run().catch(async e => {
    console.error(e)
    consumer && await consumer.disconnect()
    producer && await producer.disconnect()
    process.exit(1)
  })