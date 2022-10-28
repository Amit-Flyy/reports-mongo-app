const { MongoClient, ServerApiVersion } = require('mongodb');
require('dotenv').config();

var client;

module.exports = {
    getMongoClient: async function () {
        if (client) return client; // if it is already there, grab it here
        const uri = process.env.MONGODB_CONNECTION_STRING
        var client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true, serverApi: ServerApiVersion.v1 });
        
        // client.connect(err => {
        //     const collection = client.db("test").collection("devices");
        //     // perform actions on the collection object
        //     console.log(collection);
        //     client.close();
        // });

        return client;
    }
};

// const { MongoClient, ServerApiVersion } = require('mongodb');
// const uri = "mongodb+srv://kumar-amit-account:mongo-pass@cluster0.ciwey77.mongodb.net/?retryWrites=true&w=majority";
// const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true, serverApi: ServerApiVersion.v1 });
// client.connect(err => {
//   const collection = client.db("test").collection("devices");
//   // perform actions on the collection object
//   client.close();
// });