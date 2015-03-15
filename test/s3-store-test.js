var assert = require('assert');
var should = require('should');
var color = require('colors');
var util = require('util');
var fs = require('fs');

var http = require('http');

var traverse = require('optimuslime-traverse');

//handle put requests for objects to s3
// var request = require('superagent');

var Q = require('q');

var AWS = require('aws-sdk');
var redis = require('redis');
var S3StoreClass = require("../lib/s3-store.js");

var cuid = require('win-utils').cuid;

var next = function(range)
{
    return Math.floor((Math.random()*range));
};

var redisClient;
var s3StoreObject;

var ins = function(obj, val)
{
    return util.inspect(obj, false, val || 10);
}


function asyncTestS3(s3, params)
{
    var defer = Q.defer();

    s3.upload(params, function(err, data) {
     if (err) {
         // console.log("Error uploading data: ", err);
         defer.reject(err);
     } else {
        defer.resolve();
     }
    });
    return defer.promise;
}

function asyncGetS3(location, bucket)
{
     var defer = Q.defer();

        var options = {
          host: bucket + ".s3.amazonaws.com",
          port: 80,
          path: location,
          headers: {
            // "User-Agent": "curl/7.30.0",
            // "Accept" : "*/*"
          },
          method: 'GET'
        };

        var req = http.request(options, function(res) {
          // console.log('STATUS: ' + res.statusCode);
          // console.log('HEADERS: ' + JSON.stringify(res.headers));
          res.setEncoding('utf8');

        var ret = "";
          res.on('data', function (chunk) {
            // console.log('BODY: ' + chunk);
            ret += chunk;
          });

          res.on('end', function()
          {
              if(res.statusCode != 200)
              {
                defer.reject(res);
              }
              else
                defer.resolve(ret);
          })

        });

        req.on('error', function(e) {
          console.log('problem with request: ' + e.message);
          defer.reject(e);
        });

        req.end();

    return defer.promise;   
}

function asyncUploadS3(value, location, bucket, expires)
{
    var defer = Q.defer();

        var options = {
          host: bucket + ".s3.amazonaws.com",
          port: 80,
          path: location,
          headers: {
            "User-Agent": "curl/7.30.0",
            "Accept" : "*/*",
            // "x-amz-acl" : "public-read",
            // "Content-Type" : "application/json",
            "Content-Length" : Buffer.byteLength(value)
          },
          method: 'PUT'
        };

        var req = http.request(options, function(res) {
          // console.log('STATUS: ' + res.statusCode);
          // console.log('HEADERS: ' + JSON.stringify(res.headers));
          res.setEncoding('utf8');

          if(res.statusCode != 200)
          {
            defer.reject(res);
          }
          else
            defer.resolve();

          res.on('data', function (chunk) {
            // console.log('BODY: ' + chunk);
          });

        });

        req.on('error', function(e) {
          console.log('problem with request: ' + e.message);
          defer.reject(e);
        });

        req.write(value);
        req.end();

        // console.log("\t Making request: ".cyan, location, " with value: ".blue, value, " buck: ", bucket, "\n\n\n Options: ".red, options, "\n\n\n\n request ".rainbow, req);

    //where we goin with it???
    // request
    //     .put(location)
    //     // .set('X-API-Key', 'foobar')
    //     // .set("x-amz-acl", "public-read")
    //     // .set('Content-Type', 'application/json')
    //     // .set('Expires', expires)
    //     .send(value)
    //     .set('Content-Type', "application/octet-stream")
    //     .set('Host', bucket + ".s3.amazonaws.com")
    //     .set("Accept", "*/*")
    //     .set('Content-Length', Buffer.byteLength(value))
    //     .end(function(err, res){
            
    //         console.log('Amz responded!'.green, err);

    //         if(!err)
    //             defer.resolve(res);
    //         else
    //             defer.reject(err);
    //     });

    return defer.promise;
}

describe('Testing S3 Store API -',function(){

    //we need to start up the WIN backend

    before(function(done){

        redisClient = redis.createClient();

        redisClient.on('error', function(err)
        {
            done(err);
        })
        redisClient.on('ready', function()
        {
             done();
        })
    });

     it('S3 loading/comlpete simple check',function(done){

        this.timeout(15000);
        
        var configLocation = __dirname + "/../access-credentials.json";
          //grab our access info
        var accessInfo = JSON.parse(fs.readFileSync(configLocation));


        console.log('\t S3 Store Config Info:  '.blue, accessInfo);

        //load in our config info
        AWS.config.loadFromPath(configLocation);
        
        //set region plz
        if(!AWS.config.region)
            AWS.config.region = 'us-east-1';

        var createFiles = function(props)
        {
            //we will create 4 files -- pulling info from props
            var user = props.user;

            var prepend = user + "/";

            return [
                {prepend: prepend, append: "/smallImage"},
                {prepend: prepend, append: "/mediumImage"},
                {prepend: prepend, append: "/largeImage"},
                {prepend: prepend, append: "/xLargeImage"}
            ];
        }

        //create our s3 storage object
        s3StoreObject = new S3StoreClass(accessInfo, createFiles, redisClient);

        //grab bucket info -- this is where we're doing our read/writes
        var bucketName = accessInfo.bucket;

        //where do we access?
        var s3 = new AWS.S3({params: {Bucket: bucketName}});

        //lets make a request to initialize
        var props = {user: "bobsyouruncle"};
        

        var localUploadRequest;

        var skip = false;


        var testParams = {Key: 'tests/s3-upload-test', Body: 'Hello!'};

        console.log('\t Test S3 Bucket Upload Access: '.blue, testParams);

        asyncTestS3(s3, testParams)
            .then(function()
            {
                console.log('\t Begin inititalization: '.blue, props);
                return s3StoreObject.asyncInitializeUpload(props);
            })
            .then(function(uploadInfo)
            {
                //we have info on uploads
                //lets complete them!

                console.log('\t Uploads inititalized: '.magenta, uploadInfo);

                //save whole request
                localUploadRequest = uploadInfo;

                //we should have the uploads
                var fullUploads = uploadInfo.uploads;

                //lets do the uploads
                var promised = [];

                for(var i=0; i < fullUploads.length; i++)
                {
                    var upReq = fullUploads[i];

                    promised.push(asyncUploadS3(JSON.stringify({image: i}), upReq.url, accessInfo.bucket));
                }

                return Q.allSettled(promised);
            })
            .then(function(results)
            {
                //we've sent it
                //now confirm it's done
                console.log("\t Results from uploads: ".blue, ins(results,1));

                //check if done
                var missing = [];
                for(var i=0; i < results.length; i++)
                {
                    var res = results[i];
                    if(res.state !== "fulfilled")
                        missing.push(i);
                }

                if(missing.length)
                    throw new Error("Uplaods failed" + JSON.stringify(missing));

                //otherwise, we check if completed
                return s3StoreObject.asyncConfirmUploadComplete(localUploadRequest.uuid);
            })
            .then(function(res)
            {
                //now we verify it worked!
                res.success.should.equal(true);

                done();
            })
            .catch(function(err)
            {
                console.log('\t Error detected: '.red, err);

                done(new Error(err));
            })
    });

      it('S3 loading/complete should fail',function(done){

        this.timeout(15000);
        
        var configLocation = __dirname + "/../access-credentials.json";
          //grab our access info
        var accessInfo = JSON.parse(fs.readFileSync(configLocation));


        console.log('\t S3 Store Config Info:  '.blue, accessInfo);

        //load in our config info
        AWS.config.loadFromPath(configLocation);
        
        //set region plz
        if(!AWS.config.region)
            AWS.config.region = 'us-east-1';

        var createFiles = function(props)
        {
            //we will create 4 files -- pulling info from props
            var user = props.user;

            var prepend = user + "/";

            return [
                {prepend: prepend, append: "/smallImage"},
                {prepend: prepend, append: "/mediumImage"},
                {prepend: prepend, append: "/largeImage"},
                {prepend: prepend, append: "/xLargeImage"}
            ];
        }

        //create our s3 storage object
        s3StoreObject = new S3StoreClass(accessInfo, createFiles, redisClient);

        //grab bucket info -- this is where we're doing our read/writes
        var bucketName = accessInfo.bucket;

        //where do we access?
        var s3 = new AWS.S3({params: {Bucket: bucketName}});

        //lets make a request to initialize
        var props = {user: "bobsyouruncle"};
        

        var localUploadRequest;

        var skip = false;


        var testParams = {Key: 'tests/s3-upload-test', Body: 'Hello!'};

        console.log('\t Test S3 Bucket Upload Access: '.blue, testParams);

        asyncTestS3(s3, testParams)
            .then(function()
            {
                console.log('\t Begin inititalization: '.blue, props);
                return s3StoreObject.asyncInitializeUpload(props);
            })
            .then(function(uploadInfo)
            {
                //we have info on uploads
                //lets complete them!

                console.log('\t Uploads inititalized: '.magenta, uploadInfo);

                //save whole request
                localUploadRequest = uploadInfo;

                //we should have the uploads
                var fullUploads = uploadInfo.uploads;

                //lets do the uploads
                var promised = [];

                for(var i=0; i < fullUploads.length - 1; i++)
                {
                    var upReq = fullUploads[i];

                    promised.push(asyncUploadS3(JSON.stringify({image: i}), upReq.url, accessInfo.bucket));
                }

                return Q.allSettled(promised);
            })
            .then(function(results)
            {
                //we've sent it
                //now confirm it's done
                console.log("\t Results from uploads: ".blue, ins(results,1));

                //check if done
                var missing = [];
                for(var i=0; i < results.length; i++)
                {
                    var res = results[i];
                    if(res.state !== "fulfilled")
                        missing.push(i);
                }

                if(missing.length)
                    throw new Error("Uplaods failed" + JSON.stringify(missing));

                //otherwise, we check if completed
                return s3StoreObject.asyncConfirmUploadComplete(localUploadRequest.uuid);
            })
            .then(function(res)
            {

                //now we verify it worked!
                res.success.should.equal(false);

                //we should be missing some objects, plz!
                res.error.should.equal(s3StoreObject.uploadErrors.headCheckMissingError);

                //all confirmed
                console.log("\t Confirmed response should fail: ".green, res);

                done();
            })
            .catch(function(err)
            {
                console.log('\t Error detected: '.red, err);

                done(new Error(err));
            })
    });


    it('S3 get presigned requests',function(done){

        this.timeout(15000);
        
        var configLocation = __dirname + "/../access-credentials.json";
          //grab our access info
        var accessInfo = JSON.parse(fs.readFileSync(configLocation));

        //load in our config info
        AWS.config.loadFromPath(configLocation);
        
        //set region plz
        if(!AWS.config.region)
            AWS.config.region = 'us-east-1';

        var createFiles = function(props)
        {
            //we will create 4 files -- pulling info from props
            var user = props.user;

            var prepend = user + "/";

            return [
                {prepend: prepend, append: "/smallImage"},
                {prepend: prepend, append: "/mediumImage"},
                {prepend: prepend, append: "/largeImage"},
                {prepend: prepend, append: "/xLargeImage"}
            ];
        }

        //create our s3 storage object
        s3StoreObject = new S3StoreClass(accessInfo, createFiles, redisClient);

        //grab bucket info -- this is where we're doing our read/writes
        var bucketName = accessInfo.bucket;

        //where do we access?
        var s3 = new AWS.S3({params: {Bucket: bucketName}});

        //lets make a request to initialize
        var props = {user: "bobsyouruncle"};

        var localUploadRequest;

        var skip = false;


        var testParams = {Key: 'tests/s3-upload-test', Body: 'Hello!'};

        console.log('\t Test S3 Bucket Upload Access: '.blue, testParams);

        asyncTestS3(s3, testParams)
            .then(function()
            {
                console.log('\t Begin inititalization: '.blue, props);
                return s3StoreObject.asyncInitializeUpload(props);
            })
            .then(function(uploadInfo)
            {
                //we have info on uploads
                //lets complete them!

                console.log('\t Uploads inititalized: '.magenta, uploadInfo);

                //save whole request
                localUploadRequest = uploadInfo;

                //we should have the uploads
                var fullUploads = uploadInfo.uploads;

                //lets do the uploads
                var promised = [];

                for(var i=0; i < fullUploads.length; i++)
                {
                    var upReq = fullUploads[i];

                    promised.push(asyncUploadS3(JSON.stringify({image: i}), upReq.url, accessInfo.bucket));
                }

                return Q.allSettled(promised);
            })
            .then(function(results)
            {
                //we've sent it
                //now confirm it's done
                console.log("\t Results from uploads: ".blue, ins(results,1));

                //check if done
                var missing = [];
                for(var i=0; i < results.length; i++)
                {
                    var res = results[i];
                    if(res.state !== "fulfilled")
                        missing.push(i);
                }

                if(missing.length)
                    throw new Error("Uplaods failed" + JSON.stringify(missing));

                //otherwise, we check if completed
                return s3StoreObject.asyncConfirmUploadComplete(localUploadRequest.uuid);
            })
            .then(function(res)
            {
                //now we verify it worked!
                res.success.should.equal(true);

                //now we're finally ready to access the same objects again!
                 var fullUploads = localUploadRequest.uploads;

                 var fileLocations = [];

                 for(var i=0; i < fullUploads.length; i++)
                 {
                    fileLocations.push(fullUploads[i].request.Key);
                 }

                 console.log('\t Fetching file access: '.cyan, fileLocations);

                 var accessURLs = s3StoreObject.generateObjectAccess(fileLocations);

                 //lets get all the objects!
                 var promised = [];

                for(var i=0; i < fileLocations.length; i++)
                {
                    var getURL = accessURLs[fileLocations[i]];

                    promised.push(asyncGetS3(getURL, accessInfo.bucket));
                }

                return Q.allSettled(promised);
            })
            .then(function(results)
            {

                console.log('\t GET Return Values: '.cyan, results);

                var objects = {};

                var missing = [];
                for(var i=0; i < results.length; i++)
                {
                    var res = results[i];
                    if(res.state !== "fulfilled")
                        missing.push(i);
                    else
                        objects[i] = JSON.parse(res.value);
                }

                if(missing.length)
                    throw new Error("GETs Failed");

                //now we need to look and validate all of them
                objects[0].image.should.equal(0);
                objects[1].image.should.equal(1);
                objects[2].image.should.equal(2);
                objects[3].image.should.equal(3);

                done();

            })
            .catch(function(err)
            {
                console.log('\t Error detected: '.red, err);

                done(new Error(err));
            })


    });
});