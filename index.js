const express = require('express')
const bodyParser = require('body-parser')
const app = express()
app.use(bodyParser.json())

const Database = require("@replit/database")
// Suscripciones irán con prefijo 'sub_' en base de datos
const subscriptionsDb = new Database()

const crypto = require('crypto')
const _ = require('underscore')
const axios = require('axios')


// Home
app.get('/', function(req, res) {
  res.send('hola chao')
})


// Registra medición
app.post('/measures', async function(req, res) {
  const sensorId = req.body.sensorId
  const timestamp = req.body.timestamp
  const value = req.body.value

  // Identificador de cada mensaje, idealmente único
  const messageId = `${sensorId}_${timestamp}`

  // Revisa todas las suscripciones
  let suscriptions = await subscriptionsDb.list("sub_")
  _(suscriptions).each(async function(subscriptorKey) {
    // Lee datos actuales del suscriptor
    let sub = await subscriptionsDb.get(subscriptorKey)
    if (sub) {
      // Agrega la medición actual a los pendientes
      sub.pendingMessages[messageId] = {sensorId, timestamp, value}
      //console.log(sub)

      for (const [messageId, message] of Object.entries(sub.pendingMessages)) { 
        //console.log(`messageId: ${messageId}`)

        try {
          let response = await axios.post(sub.endpoint, message)
          //console.log(`response: ${JSON.stringify(response.body)}`)

          // Quita mensaje de lista de pendientes
          delete sub.pendingMessages[messageId]
          //console.log(`mensaje enviado: ${messageId}`)
        }
        catch (error) {
          // En todos los casos de error, no retira el mensaje
          // de la fila, asi que reintentará después

          let status = error.response.status
          if (status == 500) {
            console.error(`Error: Error interno`)
          }
          else if (status == 405) {
            console.error(`Error: Datos incorrectos`)
          }
          else {
            // Sin mensaje especial en otros casos...
          }
        }
      }

      //console.log(`pendientes: ${JSON.stringify(sub.pendingMessages)}`)

      // Sobreescribe en base de datos los detalles del suscriptor
      await subscriptionsDb.set(subscriptorKey, {
        endpoint: sub.endpoint,
        pendingMessages: sub.pendingMessages,
      })
    }
  })

  res.send(JSON.stringify({message: 'ok', current: {sensorId, timestamp, value}}))
})


// Registra suscripción
app.post('/subscriptions', async function(req, res) {
  const endpoint = req.body.endpoint

  // Llave para usar en store de suscripciones
  let key = crypto.createHash('sha256').update(endpoint).digest('hex')
  //console.log(`key: ${key}`)

  var value = await subscriptionsDb.get(`sub_${key}`)
  if (!value) {
    // Registro inicial de suscriptor
    let newValue = {
      endpoint: endpoint, // Regista el endpoint a llamar después
      pendingMessages: {} // Estructura para guardar mensajes pendientes
    }
    await subscriptionsDb.set(`sub_${key}`, newValue)
    value = newValue
  }

  //console.log(`val: ${JSON.stringify(value, null, 2)}`)

  res.send(JSON.stringify({message: 'ok', endpoint: endpoint}))
})


// Utilitario para borrar base de datos
app.get('/__unsubscribe_all', async function(req, res) {
  await subscriptionsDb.empty()
  res.send(JSON.stringify({message: 'ok', keys: await subscriptionsDb.list()}))
})


// Inicializa servicio
const port = 3000
app.listen(port, () => {
  console.log(`Listening at http://localhost:${port}`)
})
