console.log('~~~kafka consumer api test~');

var Kafka = require('node-rdkafka');

var consumer = new Kafka.KafkaConsumer({
	'group.id':'kafka',
	'metadata.broker.list':'172.31.36.117:9000,172.31.45.128:9000,172.31.35.220:9000',
	// 'metadata.broker.list':'localhost:9000,172.31.45.128:9000,172.31.35.220:9000',
});

var topics =['speedshield', 'speedshield001'];

consumer.on('event.log',function(log){
	console.log('~~event~~',log);
});

consumer.on('event.err',function(err){
	console.log('~~err~~',err);
});

consumer.on('ready', function(arg){
	console.log('~~consumer.ready '+JSON.stringify(arg));
	consumer.subscribe(topics);
	consumer.consume();
})

// function cbOnMsg(err, msg){
// 	if (err) {
// 		console.log('~~cbOnMsg err~',err);
// 		return;
// 	}
// 	console.log('~~cbOnMsg~',msg);
// }

consumer.on('data',function(data){
	console.log('~~data obj~~',data);	
	console.log('~~data~~',data.value.toString());
})

consumer.on('disconnect',function(arg){
	console.log('~~~consumer disconnected '+ JSON.stringify(arg));
})

consumer.connect();

// setTimeout(
// 	function(){
// 		consumer.disconnect();
// 	}
// 	,10000
// )