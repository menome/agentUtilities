
//PUT RABBIT CONFIG IN HERE
var neo4j = require('neo4j-driver').v1;
var Query = require('decypher').Query;
var config = require('./config.json');
//PUT RABBIT CONFIG IN HERE
var rabbit = require('./rabbitBomber.js')
var rmq = new rabbit(config.rabbit);
var queryBuilder = function() {
  var query = new Query();
  query.match(config.query.match)
  query.return(config.query.return)
  return query;
}

rmq.connect().then(function(){
  var neoConf=config.neo4j;
  var driver = neo4j.driver(neoConf.url, neo4j.auth.basic(neoConf.user, neoConf.pass));
  var session = driver.session();
  var query = queryBuilder();
  return session.run(query.compile(), query.params()).then(async function(result){
    driver.close();
    console.log("pulled " + result.records.length + " records from graph")
    for(var i = 0; i < result.records.length; i++){

      /****************
       * Rabbit Message definition below
       * This message needs to conform to the expected message schema
       * Ie, FPP mesage, harvesterMessage etc...
       * Copy templates below in here
     *****************/
      
   
     // Harvester Message, for sending data to a refinery (node and first degree relationships)
     var outMessage = {
       "Name": result.records[i].get("name"),
       "NodeType":config.NodeType,
       "SourceSystem": 'thelink data export',
       "Priority": 1,
       "ConformedDimensions": {
         "Uuid":result.records[i].get("uuid")
       },
       "Properties": {},
      // "Connections":[
      //   //FOllows the same general format as above, minus connections
      //   {
      //     "NodeType": "Person",
      //     "RelType": "",
      //     "ForwardRel": true,
      //     "ConformedDimensions": {
      //       "Name":itm["Manufact Name"]
      //     },
      //     "Properties": {    
      //     },
      //     "RelProps": {    
      //     }
      //   }
      // ]
     } 
     //console.log(outMessage.Properties)
     for(x in result.records[i].get("data")) 
     {
       if(result.records[i].get("data")[x] !== null && result.records[i].get("data")[x] !== '')
         outMessage.Properties[x]=JSON.stringify(result.records[i].get("data")[x])
   
    }
     // console.log(outMessage)
      var sent =  rmq.publishMessage(outMessage)
      if(sent === true)
      console.log("Sent message " + i + " to rabbit")
    }
    return rmq.disconnect();
  })
  .catch( function(err){console.log("Neo4j Error: %s",err)})
})
//process.exit();
// //FPP message format, for working on files in the library
// var outMessage = {
//   "Uuid":result.records[i].get("uuid"),
//   "Library":"miniofiles",
//   "Path":result.records[i].get("image")
// };



// // Harvester Message, for sending data to a refinery (node and first degree relationships)
// var outMessage = {
//   "Name": result.records[i].get("name"),
//   "NodeType":result.records[i].get("label"),
//   "SourceSystem": result.records[i].get("SourceSystem"),
//   "Priority": 1,
//   "ConformedDimensions": {
//     "Uuid":result.records[i].get("uuid")
//   },
//   "Properties": {
//     "Text":result.records[i].get("text")
//   },
//   "Connections":[
//     //FOllows the same general format as above, minus connections
//     {
//       "NodeType": "Person",
//       "RelType": "",
//       "ForwardRel": true,
//       "ConformedDimensions": {
//         "Name":itm["Manufact Name"]
//       },
//       "Properties": {

//       },
//       "RelProps": {

//       }
//     }
//   ]
// }