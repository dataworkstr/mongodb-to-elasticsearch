
var isFileDone = false;
var isPaused = false;
var elasticIndex='testIndex';
var elasticType='testType';
var bulkData='';
var bulkMaxCount=1000;
var currentCount = 0;
var elasticConnectionString='https://localhost:9200';
var mongoConnectionString='user:password@localhost:27000';
var mongoCollectionNames=['collectionName'];
var bar;

var helpers = require('./helpers');
var mongojs = require('mongojs');
var JSONStream = require('JSONStream');
var elasticsearch = require('elasticsearch');
var ProgressBar = require('progress');

var mongodb = mongojs(mongoConnectionString,mongoCollectionNames);

var es_client = new elasticsearch.Client({host:elasticConnectionString});

mongodb.on('error', function (err) {
	console.log('mongodb error', err)
})
 
mongodb.on('connect', function () {
	console.log('mongodb connected' + '\n')
})

var cursor = mongodb.companies.find({}, {}, {timeout: false});

cursor.count(function (err, count) {
	bar = new ProgressBar('Processing line :current of :total [:bar] :percent :elapseds', {
		width: 20,
		total: count
	});
	
	bar.render();
});

cursor.on('data', function(doc) {
	
	var docKey=''
	bar.tick();
	
	var obj = Object.create({_id:doc._id,_index:elasticIndex,_type:elasticType});
	docKey = mongoElasticKey(obj);  
	
	//dokumani stringe çevir	
	doc = JSON.stringify(doc);
	//console.log(doc);
	
	bulkData += docKey + '\n'
	bulkData += doc + '\n'
	
	currentCount++;
	if (currentCount >= bulkMaxCount) {
		cursor.pause();
		isPaused = true;
		bulkImport(function () {
			isPaused = false;
			cursor.resume();
		});
	}
	   
  });
  
  cursor.on('end', function(){
    console.log('Complete!'+ '\n');
	mongodb.close();
	process.exit();
  })
 
var mongoElasticKey = function(obj) {
	var elasticKey= '{"index":{"_index":"'+obj._index+'","_type":"'+obj._type+'","_id":"'+obj._id+'"}}';
	return elasticKey;
};  
 
var bulkImport = function (cb) {
	es_client.bulk({body: bulkData}, function (err, response) {
		if (err) {
			helpers.exit(err);
		}
		if (response.error) {
			helpers.exit('When executing bulk query: ' + response.error.toString());
		}
		
		// reset global variables
		bulkData=''
		currentCount=0;
		
		// exit or continue processing
		if (isFileDone) {
			console.log('Complete!');
		} else {
			cb();
		}
		
	});
};
  

  