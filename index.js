const cors = require('cors')
const express = require('express')
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ["my-kafka-0.my-kafka-headless.er1ck-esp1n0sa.svc.cluster.local:9092"
    //'localhost:9092'
	  //'my-kafka-0.my-kafka-headless.kafka-adsoftsito.svc.cluster.local:9092'
	  ]
});

const producer = kafka.producer()

const app = express();
app.use(cors());
app.options('*', cors());

const port = 8080;

app.get('/', (req, res, next) => {
  res.send('kafka api - Erick Juarez Espinosa');
});

/*Para las reacciones*/
const runreaction = async (username, publication_id, reaction)=>{
  await producer.connect()
  await producer.send({
    topic: 'reactions',
    messages:[{
      'value': `{"name":"${username}",
                 "publication":"${publication_id}",
                 "reaction":"${reaction}"}` 
    }],
  })
  await producer.disconnect()
}

app.get('/reaction', (req, res, next) => {
  const username = req.query.name;
  const publication_id = req.query.publication_id;
  const reaction = req.query.reaction;
  res.send({ 'name' : username,
             'publication' : publication_id,
             'reaction': reaction} );
  runreaction(username, publication_id, reaction).catch(e => console.error(`[example/producer] ${e.message}`, e))

});

/*Para los comentarios*/
const runcomments = async (username, publication_id, comment)=>{
  await producer.connect()
  await producer.send({
    topic: 'comments',
    messages:[{
      'value': `{"name":"${username}",
                 "publication":"${publication_id}",
                 "comment":"${comment}"}` 
    }],
  })
  await producer.disconnect()
}

app.get('/comment', (req, res, next) => {
  const username = req.query.name;
  const publication_id = req.query.publication_id;
  const comment = req.query.comment;
  res.send({ 'name' : username,
             'publication' : publication_id,
             'comment': comment} );
  runcomments(username, publication_id, comment).catch(e => console.error(`[example/producer] ${e.message}`, e))

});

/*const run = async (username) => {

    await producer.connect()
//    await producer.send()
    await producer.send({
      topic: 'test',
      messages: [ 
	{ 
	  'value': `{"name": "${username}" }` 
  	} 
      ],
    })
    await producer.disconnect()
}

app.get('/like', (req, res, next) => {
  const username = req.query.name;
  res.send({ 'name' : username } );
  run(username).catch(e => console.error(`[example/producer] ${e.message}`, e))

});*/

app.listen(port,  () => 
	console.log('listening on port ' + port
));