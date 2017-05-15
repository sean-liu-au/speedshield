console.log('~~~kafka consumer api test~');

var Kafka = require('node-rdkafka');

var consumer = new Kafka.KafkaConsumer({
	'group.id':'kafka',
	'metadata.broker.list':'172.31.36.117:9000,172.31.45.128:9000,172.31.35.220:9000',
});

var topicName ='speedshield';

consumer.on('event.log',function(log){
	console.log('~~event~~',log);
});

consumer.on('event.err',function(err){
	console.log('~~err~~',err);
});

consumer.on('ready', function(arg){
	console.log('consumer.ready '+JSON.stringify(arg));
	consumer.subscribe(topicName);
	consumer.consume();
})

var counter=0;
var numOfMsg =5;

consumer.on('data',function(m){
	counter++;
	if(counter%numOfMsg ===0){
		console.log(JSON.stringify(m));
		console.log(m.value.toString());
	}
})

consumer.on('disconnect',function(arg){
	console.log('consumer disconnected '+ JSON.stringify(arg));
})

consumer.connect();

setTimeout(
	function(){
		consumer.disconnect();
	}
	,10000
)