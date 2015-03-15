var fs = require('fs');
var color =require('colors');
var cuid = require('win-utils').cuid;
var Q = require('q');


// var jsonCredentials = JSON.parse(fs.readFileSync(__dirname + "/../access-credentials.json"));

//set the environment variables for nodejs -- this will be read by the aws sdk
// process.env.AWS_ACCESS_KEY_ID = jsonCredentials.accessKey;
// process.env.AWS_SECRET_ACCESS_KEY = jsonCredentials.secretKey;

// console.log("Key: " + process.env.AWS_ACCESS_KEY_ID);
// console.log("Key: " + process.env.AWS_SECRET_ACCESS_KEY);

// Load the AWS SDK for Node.js
var AWS = require('aws-sdk');

//grab config from file
// AWS.config.loadFromPath(__dirname + "/../access-credentials.json");

//set region if not set otherwise
// if(!AWS.config.region)
// 	AWS.config.region = 'us-east-1';

var exampleUploadRequestFunction = function(uploadProperties)
{
	return [{prepend: "user1/", append: "/base_name_for_put_location"}];
}
	
//we're all loaded -- now we have to access objects and store/load them
module.exports = function(configuration, createUploadRequestNamesFunction, redisClient)
{
	var self = this;

	//grab bucketname from config file
	var bucketName = configuration.bucket;
	var uploadExpiration = configuration.expires || 15*60;

	self.s3 = new AWS.S3({params: {Bucket: bucketName}, computeChecksums: false});

	//example for outside use or testing -- helpful if i forget my function logic later--or someone checking around from the outside
	self.requestFunctionExample = exampleUploadRequestFunction;
	self.createUploadRequests = createUploadRequestNamesFunction;

	self.uploadErrors = {
		nullUploadKey : 0,
		redisError : 1,
		headCheckUnfulfilledError : 2,
		headCheckMissingError : 3,
		redisDeleteError : 4
	}


	self.generateObjectAccess = function(fileLocations)
	{

		//we create a map of signed URLs
		var fileAccess = {};

		for(var i=0; i < fileLocations.length; i++)
		{
			var fLocation = fileLocations[i];

			var signedURLReq = {
				Bucket: bucketName, 
				Key: fLocation,
				Expires: uploadExpiration,
			};

			var signed = self.s3.getSignedUrl('getObject', signedURLReq);

			fileAccess[fLocation] = signed;
		}

		return fileAccess;
	}

	//in case the upload properties are failures
	self.asyncConfirmUploadComplete = function(uuid)
	{
		var defer = Q.defer();

		var isOver = false;

		//we have the uuid, lets fetch the existing request
		asyncRedisGet(uuid)
			.then(function(val)
			{
				//if we don't have a value -- the key doesn't exist or expired -- either way, it's a failure!
				if(!val)
				{
					//we're all done -- we didn't encounter an error, we just don't exist -- time to resubmit
					defer.resolve({success: false, error: self.uploadErrors.nullUploadKey});
					isOver = true;
					return;
				}

				//so we have our object
				var putRequest = JSON.parse(val);

				//these are all the requests we made
				var allUploads = putRequest.uploads;

				//we have to check on all the objects
				var uploadConfirmPromises = [];

				for(var i=0; i < allUploads.length; i++)
				{
					var req = allUploads[i].request;

					var params = {Bucket: req.Bucket, Key: req.Key};

					uploadConfirmPromises.push(asyncHeadRequest(params));
				}

				return Q.allSettled(uploadConfirmPromises);
			})
			.then(function(results)
			{
				if(isOver)
					return;

				//check for existance
				var missing = [];

				//now we have all the results -- we verify they're all fulfilled
				for(var i=0; i < results.length; i++)
				{
					var res = results[i];
					if(res.state !== "fulfilled")
					{
						defer.reject({success: false, error: self.uploadErrors.headCheckUnfulfilledError});
						isOver = true;
						return;
					}

					//if one is false, we're all false
					if(!res.value.exists)
						missing.push(i);

				}

				//are we missing anything???
				if(missing.length)
				{
					defer.reject({success: false, error: self.uploadErrors.headCheckMissingError, missing: missing});
					isOver = true;
					return;
				}

				//we're all confirmed, we delete the upload requests now
				//however, this is not a major concern -- it will expire shortly anyways
				//therefore, we resolve first, then delete second

				isOver = true;
				defer.resolve({success: true});

				//if we're here, then we all exist -- it's confirmed yay!
				//we need to remove the key from our redis client -- but we're not too concerned 
				return asyncRedisDelete(uuid);
			})
			.catch(function(err)
			{
				if(isOver)
					return;

				defer.reject(err);
			});

		return defer.promise;
	}

	function asyncHeadRequest(params)
	{
		var defer = Q.defer();

		self.s3.headObject(params, function (err, metadata) {  
			if (err)
			{
				//error code is not found -- it doesn't exist!
				if(err.code === 'Not Found') {  
			    	// Handle no object on cloud here  
					defer.resolve({exists: false});
				} 
				//straight error -- reject
				else 
					defer.reject(err);

			}else {  
				defer.resolve({exists: true});
			}
		});

		return defer.promise;
	}


	self.asyncInitializeUpload = function(uploadProperties)
	{	
		//we promise to return
		var defer = Q.defer();

		//create a unique ID for this upload
		var uuid = cuid();

		//we send our upload properties onwards
		var requestUploads = self.createUploadRequests(uploadProperties);

		//we now have a number of requests to make
		//lets process them and get some signed urls to put the objects there
		var fullUploadRequests = [];

		//these will be everything we need to make a signed URL request
		for(var i=0; i < requestUploads.length; i++)
		{

			//
			var upReqName = requestUploads[i];

			var signedURLReq = {
				Bucket: bucketName, 
				Key: upReqName.prepend + uuid + upReqName.append, 
				Expires: uploadExpiration,
				// ACL: 'public-read'
			};

			var signed = self.s3.getSignedUrl('putObject', signedURLReq);

			//store the requests here
			fullUploadRequests.push({url: signed, request: signedURLReq});
		}

		//now we have our full requests
		//let's store it in REDIS, then send it back

		var inProgress = {
			uuid: uuid,
			state : "pending",
			uploads: fullUploadRequests
		};

		//we set the object in our redis location
		asyncRedisSetEx(uuid, uploadExpiration, JSON.stringify(inProgress))
			.catch(function(err)
			{
				defer.reject(err);
			})
			.done(function()
			{
				defer.resolve(inProgress);
			});

		//promise for now -- return later
		return defer.promise;
	}

	function asyncRedisSetEx(key, expire, val)
	{
		var defer = Q.defer();

		redisClient.setex(key, expire, val, function(err)
		{
			if(!err)
				defer.resolve();
			else
				defer.reject(err);
		});

		return defer.promise;
	}

	function asyncRedisGet(key, val)
	{
		var defer = Q.defer();

		redisClient.get(key, function(err, val)
		{
			if(!err)
				defer.resolve(val);
			else
				defer.reject(err);
		});

		return defer.promise;
	}

	function asyncRedisDelete(key, val)
	{
		var defer = Q.defer();

		redisClient.del(key, function(err, reply)
		{
			if(!err)
				defer.resolve();
			else
				defer.reject(err);
		});

		return defer.promise;
	}

	return self;
}

// var s3bucket = 

// var params = {Key: 'myKey', Body: 'Hello!'};

// s3bucket.upload(params, function(err, data) {
// 	if (err) {
// 		console.log("Error uploading data: ", err);
// 	} else {
// 		console.log("Successfully uploaded data to myBucket/myKey");
// 	}
// });

// s3bucket.listBuckets(function(err, data) {

// 	console.log("\t lists Bucket return args: ".blue, arguments);

// 	if (err) { console.log("Error:", err); }
// 	else {
// 		for (var index in data.Buckets) {
// 			var bucket = data.Buckets[index];
// 			console.log("Bucket: ", bucket.Name, ' : ', bucket.CreationDate);
// 		}
// 	}
// });

// s3bucket.createBucket(function() {

// 	console.log("\t create Bucket return args: ".blue, arguments);

//   var params = {Key: 'myKey', Body: 'Hello!'};
//   s3bucket.upload(params, function(err, data) {
//     if (err) {
//       console.log("Error uploading data: ", err);
//     } else {
//       console.log("Successfully uploaded data to myBucket/myKey");
//     }
//   });
// });