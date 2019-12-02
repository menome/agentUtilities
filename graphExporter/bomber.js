
//PUT RABBIT CONFIG IN HERE
var neo4j = require('neo4j-driver').v1;
var Query = require('decypher').Query;
var config = require('./config.json');
//PUT RABBIT CONFIG IN HERE
var rabbit = require('./rabbitmq.js')
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
    var uuidList = [];
    for(var i = 0; i < result.records.length; i++){

      /****************
       * Rabbit Message definition below
       * This message needs to conform to the expected message schema
       * Ie, FPP mesage, harvesterMessage etc...
       * Copy templates below in here
     *****************/
      console.log(result.records[i])
      itm = result.records[i]

      n1 = itm._fields[0]
      r  = itm._fields[1]
      n2 = itm._fields[0]
      // Harvester Message, for sending data to a refinery (node and first degree relationships)
      var outMessage = {
        "NodeType":n1.labels,
        "SourceSystem": 'thelink data export',
        "Priority": 1,
        "ConformedDimensions": {
          "Uuid":n1.properties.Uuid
        },
        "Properties": {},
        "Connections":[
          {
            "NodeType": n2.labels,
            "RelType": r.type,
            "ForwardRel": true ? r.start == n1.identity : false,
            "ConformedDimensions": {
              "Uuid":n2.properties.Uuid
            },
            "Properties": {},
            "RelProps": {}
          }
        ]
      } 

      //Add n1 properties
      if(uuidList.indexOf(n1.properties.Uuid) == -1){
        uuidList.add(n1.properties.Uuid)
        for(x in n1.properties) {
          if(n1.properties[x] !== null && n1.properties[x] !== '')
          outMessage.Properties[x]=str(n1.properties[x])
        }
      }
      //add REL properties
      for(x in r.properties) {
        if(r.properties[x] !== null && r.properties[x] !== '')
        outMessage.Connections[0].RelProps[x]=str(r.properties[x])
      }
      //add n2 properties
      if(uuidList.indexOf(n2.properties.Uuid) == -1){
        uuidList.add(n2.properties.Uuid)
        for(x in n2.properties) {
          if(n2.properties[x] !== null && n2.properties[x] !== '')
          outMessage.Connections[0].Properties[x]=str(n2.properties[x])
        }
      }
      console.log(outMessage)
      // var sent =  rmq.publishMessage(outMessage)
      // if(sent === true)
      // console.log("Sent message " + i + " to rabbit")
    }
    return rmq.disconnect();
  })
  .catch( function(err){console.log("Neo4j Error: %s",err)})
})

// cleans off the darn extra quotes added by stringify
const str = (s) => {
  let ret = JSON.stringify(s);
  if (ret.indexOf('"') === 0) {
    ret = ret.slice(1);
  }
  if (ret[ret.length - 1] === '"') {
    ret = ret.slice(0, -1 + ret.length);
  }
  return ret;
};
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