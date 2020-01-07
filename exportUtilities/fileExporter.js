
//PUT RABBIT CONFIG IN HERE
var neo4j = require('neo4j-driver').v1;
var Query = require('decypher').Query;
const fs = require('fs');
var Jimp = require('jimp');
const helpers = require('./helpers');
const fs = require('fs');
const request = require('request');
const rp = require('request-promise');
const URL = require('url').URL;

  // Librarin Config
  var librarianConfig= {
    "enable": true,
    "host": "http://localhost:10004",
    "username": "botuser",
    "password": "CodaGlenBaronyMonk"
  }
  //NEO4J CONFIG
  var neoConf={
    "url": "bolt://localhost:10002",
    "user": "neo4j",
    "pass": "CodaGlenBaronyMonk",
  }
  var driver = neo4j.driver(neoConf.url, neo4j.auth.basic(neoConf.user, neoConf.pass));
  var session = driver.session();

 //NEO4J QUERY
 var queryBuilder = function() {
    var query = new Query();
    query.match(" (p:Page)-[]-(t:Table) with p,t ")
    // query.match("(p:Page)-[:HAS_FACET]-(pr:Processing {Name:'Table Extraction Failed'}) where p.HasTable=true  with collect(distinct p) as chemistryPages unwind chemistryPages as p")
    //query.match("(p:Page)-[:HAS_CHEMISTRY]-(i:Instance) where p.HasTable=true and not exists(p.textractSuccess) with collect(distinct p) as chemistryPages unwind chemistryPages as p")
    query.return("split(t.Filename,'.')[0] + '-page-' + t.PageNumber +'.csv' as filename, t.CSV as csv")
    return query;
  }



  var query = queryBuilder();
  return session.run(query.compile(), query.params()).then(async function(result){
    driver.close();
    console.log("pulled " + result.records.length + " records from graph")
    for(var i = 0; i < result.records.length; i++){
      //get data
      var filename =result.records[i].get("filename")
      var data=result.records[i].get("csv")

      // get image for table pulled
      var filePath= "textract-" + msg.Uuid + ".png"
      return getFile(msg.Library, msg.Path, filePath).then((filePath)=>{
        return this.sizeImage(filePath)

      })
      console.log(filename + ' ' +data)
     
  }

  // Get the file from the Librarian. Returns a promise with a path to the file.
  // If the file already exists, don't re-download it.
  this.getFile = function(libkey, libpath, localpath) {
    if(fs.existsSync(localpath)){
      return Promise.resolve(localpath);
    }else
      return download(libkey, libpath, localpath)
  }

  this.sizeImage = function(filePath){
    return new Promise((resolve,reject) => {
      if(fs.statSync(filePath).size > 1024000){
        Jimp.read(filePath).then(file => {
          file.quality(60).greyscale().writeAsync(filePath.slice(0,-4)+".jpg")
          helpers.deleteFile(filePath)
          return resolve(filePath.slice(0,-4)+".jpg")
        })
        .catch(err => {
          return reject(err);
        });
      }else{
        return resolve(filePath);
      }
    })
  }
    this.download = (key,path,filePath) => {
    var url =  new URL('/file', config.host);
    
    url.searchParams.set("library",key);
    url.searchParams.set("path",path);
    var opts = {
      url: url.toString(),
      qs: {
        library: key,
        path: path
      },
      auth: {
        user: config.username,
        pass: config.password
      }
    }

    return new Promise((resolve,reject) => {
      request(opts)
        .on('error', (err) => {
          return reject(err);
        })
        .pipe(fs.createWriteStream(filePath))
        .on('error', (err) => {
          return reject(err);
        })
        .on('finish', () => {
          return resolve(filePath);
        })
    });
  }

  this.initialize = function(){
  }

})


