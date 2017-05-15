console.log('~~~kafka api test~');

var Kafka = require('node-rdkafka');

var producer = new Kafka.Producer({
	'metadata.broker.list':'172.31.36.117:9000,172.31.45.128:9000,172.31.35.220:9000',
	'dr_cb':true,
	'event_cb':true
});

producer.connect({},function(err){
	if (err) {
		console.error(err);
		return process.exit(1);
	}
});	


producer.on('ready',function(){
	try{
		producer.produce(
			'topic',
			null,
			new Buffer('msg from node app'),
			'stormwind',
			Date.now()
		);				
	}catch(err){
		console.error('A problem happened when sending message');
		console.error(err);	
	}

});


producer.on('event.error',function(error){
	console.error('Error from producer');
	console.error(err);
});

