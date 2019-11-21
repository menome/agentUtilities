
//PUT RABBIT CONFIG IN HERE
var neo4j = require('neo4j-driver').v1;
var Query = require('decypher').Query;
const fs = require('fs');

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
      console.log(filename)
      // write to file
      fs.writeFile(filename, data, (err) => {
        if (err) console.log(err);
        console.log("Successfully Written to File.");
      });

      }
      console.log(filename + ' ' +data)
     
  })


