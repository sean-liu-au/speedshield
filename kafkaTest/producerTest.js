console.log('~~~kafka producer api test~');

var Kafka = require('node-rdkafka');

var producer = new Kafka.Producer({
	'metadata.broker.list':'172.31.36.117:9000,172.31.45.128:9000,172.31.35.220:9000',
	'dr_cb':true,
	'event_cb':true
});

producer.setPollInterval(100);

producer.on('delivery-report',function(err,report){
	console.log('~~delivery-report ',JSON.stringify(report));
})

producer.on('ready',function(arg){
	try{
		console.log('~~~connected to kafka~~'+ JSON.stringify(arg));

		var topic = producer.Topic('speedshield001',{'request.required.acks':1});

		var time=Date.now();
		console.log('~~ sending ~~'+time);
		producer.produce(
			topic,
			-1,
			new Buffer('msg from test at '+time),
			'key - '+time
		);				

	}catch(err){
		console.log('A problem happened when sending message');
		console.log(err);	
	}

});


producer.on('event.error',function(error){
	console.error('Error from producer');
	console.error(err);
});

producer.on('disconnected', function(arg){
	console.log('~~ producer disconnected '+ JSON.stringify(arg));
})


producer.connect({},function(err){
	if (err) {
		console.error(err);
		return process.exit(1);
	}
});	

