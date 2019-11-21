
//PUT RABBIT CONFIG IN HERE
var neo4j = require('neo4j-driver').v1;
var Query = require('decypher').Query;
const fs = require('fs');
var Jimp = require('jimp');
const helpers = require('./helpers');
const Bot = require('@menome/botframework');

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
      var filePath= bot.config.get("textract").mount + "textract-" + msg.Uuid + ".png"
      bot.logger.info("Analyzing file: " + msg.Path)
      return getFile(bot, msg.Library, msg.Path, filePath).then((filePath)=>{
        return this.sizeImage(filePath)

      })
      console.log(filename + ' ' +data)
     
  }

  // Get the file from the Librarian. Returns a promise with a path to the file.
  // If the file already exists, don't re-download it.
  this.getFile = function(bot, libkey, libpath, localpath) {
    if(fs.existsSync(localpath)){
      bot.logger.info("File Exists " + localpath)
      return Promise.resolve(localpath);
    }else
      return bot.librarian.download(libkey, libpath, localpath)
  }

  this.sizeImage = function(filePath){
    return new Promise((resolve,reject) => {
      if(fs.statSync(filePath).size > 1024000){
        bot.logger.info("resizing image")
        Jimp.read(filePath).then(file => {
          file.quality(60).greyscale().writeAsync(filePath.slice(0,-4)+".jpg")
          helpers.deleteFile(filePath)
          return resolve(filePath.slice(0,-4)+".jpg")
        })
        .catch(err => {
          bot.logger.error(err);
          return reject(err);
        });
      }else{
        return resolve(filePath);
      }
    })
  }
  this.initialize = function(){
  }

})


