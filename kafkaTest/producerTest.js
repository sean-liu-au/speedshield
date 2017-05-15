console.log('~~~kafka producer api test~');

var Kafka = require('node-rdkafka');

var producer = new Kafka.Producer({
	'metadata.broker.list':'172.31.36.117:9000,172.31.45.128:9000,172.31.35.220:9000',
	'dr_cb':true,
	'event_cb':true
});

producer.on('delivery-report',function(err,report){
	console.log('~~delivery-report ',JSON.stringify(report));
})

producer.on('ready',function(){
	try{
		console.log('~~~connected to kafka~~');

		for (var i =0; i <10; i++) {
			console.log('~~~'+i);
			producer.produce(
				'speedshield',
				-1,
				new Buffer('msg from speedshield value '+i),
				'key - '+i
			);				
		}
	}catch(err){
		console.error('A problem happened when sending message');
		console.error(err);	
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

