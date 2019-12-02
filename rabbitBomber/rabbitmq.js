/*
 * Copyright (C) 2017 Menome Technologies Inc.
 *
 * Manages connection to RabbitMQ.
 */
"use strict";
var amqp = require('amqplib');

// Constructor for Rabbit interface.
module.exports = function rabbit(config){
  this.config = config

  var rabbitConnectInterval;
  var rabbitChannel;

  // Subscribes to the RabbitMQ 
  this.connect = function() {
    rabbitConnectInterval = setInterval(rabbitConnect, 5000);
    return rabbitConnect();
  }

  // Private message to actually get in there.
  function rabbitConnect() {
    console.log("Attempting to connect to RMQ." + config.url);
    return amqp.connect(config.url)
      .then(function(conn) {
        conn.on('error', function(err) {
          console.log(err);
          conn.close();
          rabbitChannel = null;
          rabbitConnectInterval = setInterval(rabbitConnect, 5000);
        });
        console.log("Connected to RMQ");
        return conn.createChannel();
      })
      .then(function(channel) {
        console.log("Created channel")
        //channel.prefetch(config.prefetch); // Set our prefetch value.
        clearInterval(rabbitConnectInterval); // Stop scheduling this task if it's finished.
        rabbitChannel = channel;
        return true
      })
      .catch(function(err){
        console.log("Error connecting to rabbit: ", err);
      })
  }

  // Allow us to publish a message.
  // Optionally validate against a schema for some additional integrity.
  this.publishMessage = function(msg,schemaName,{routingKey, exchange}={}) {
    if(!rabbitChannel) return Promise.resolve(false);
    if(!routingKey) routingKey = config.routingKey;
    if(!exchange) exchange = config.exchange;

    if(schemaName) {
      var errors = schema.validate(schemaName, msg);
      if (errors) {
        console.log("Attempted to publish a malformed message:", errors);
        return Promise.resolve(false);
      }
    }

    var messageBuffer = new Buffer.from(JSON.stringify(msg));
    //TODO: Handle when publish queues messages due to full buffers.
    return rabbitChannel.publish(exchange,routingKey,messageBuffer)
  }

  this.disconnect = function() {
      console.log("disconnecting.")
    if(rabbitChannel) rabbitChannel.cancel("tmbot");
    clearInterval(rabbitConnectInterval);
  }

}
