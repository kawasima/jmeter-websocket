"use strict";
 
process.title = 'chat';
 
var webSocketsServerPort = 9090;
 
var webSocketServer = require('websocket').server;
var http = require('http');
 
var history = [ ];
var clients = [ ];

var server = http.createServer(function(request, response) {
});

server.listen(webSocketsServerPort, function() {
    console.log((new Date()) + " Server is listening on port " + webSocketsServerPort);
});
 
var wsServer = new webSocketServer({
    httpServer: server
});
 
wsServer.on('request', function(request) {
    var query = request.resourceURL.query;
    var name = query["name"] || "unknown";

    var connection = request.accept(null, request.origin);
    var index = clients.push(connection) - 1;

    if (history.length > 0) {
        connection.sendUTF(JSON.stringify(history));
    }
 
    connection.on('message', function(message) {
        if (message.type !== 'utf8')
            return;

        var message = message.utf8Data;

        var chat = {
            name: name,
            message: message,
            chatedAt: new Date()
        };
        history.push(chat);
        history = history.slice(-100);

        var json = JSON.stringify([chat]);
        for (var i=0; i < clients.length; i++) {
            clients[i].sendUTF(json);
        }
    });
 
    connection.on('close', function(connection) {
        console.log((new Date()) + " User "
            + name + " disconnected.");
        clients.splice(index, 1);
    });
});