
//PUT RABBIT CONFIG IN HERE
var neo4j = require('neo4j-driver').v1;
var Query = require('decypher').Query;
const fs = require('fs');
var Jimp = require('jimp');
const request = require('request');
const rp = require('request-promise');
const URL = require('url').URL;

// Librarin Config
var librarianConfig= {
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
  query.match("  (i:Instance)-[:HAS_CHEMISTRY]-(p)-[]-(t:Table) where t.Download=false \
  with distinct t limit 50\
  match (s:Site)-[]-(f:File)-[]-(p:Page)-[]-(t) where s.Name='Montrose' \
  with distinct t set t.Download=true with t") 
  query.return("t.LibraryKey as Library, t.Image as Path, split(t.Filename,'.')[0] + '-page-' + t.PageNumber + '.png' as Name ")
  return query;
}


var download = (key,path,filePath) => {
  var url =  new URL('/file', librarianConfig.host);
  
  url.searchParams.set("library",key);
  url.searchParams.set("path",path);
  var opts = {
    url: url.toString(),
    qs: {
      library: key,
      path: path
    },
    auth: {
      user: librarianConfig.username,
      pass: librarianConfig.password
    }
  }
  console.log(opts)
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
// Get the file from the Librarian. Returns a promise with a path to the file.
// If the file already exists, don't re-download it.
var getFile = function(libkey, libpath, localpath) {
  // if(fs.existsSync(localpath)){
  //   return Promise.resolve(localpath);
  // }else
    return download(libkey, libpath, localpath)
}
//start
var query = queryBuilder();
return session.run(query.compile(), query.params())
.then(async function(result){
  driver.close();
  console.log("pulled " + result.records.length + " records from graph")
  for(var i = 0; i < result.records.length; i++){
    //get data
    var lib =result.records[i].get("Library")
    var path =result.records[i].get("Path")
    var name = result.records[i].get("Name")
    // get image for table pulled
    getFile(lib, path, "output/"+name).then((filePath)=>{
      console.log(filePath)
    })
    
  }
})
.catch( function(err){console.log("Error: %s",err)})


