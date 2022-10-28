const express = require('express');
require('dotenv').config()

const app = express();

app.get("/server_up", (req, res) => {
    console.log("Server working");
    res.send("Howdy")
})

const mongo_client = require('./database/mongo_connection');
const pg_client = require('./database/pg_pool');

async function getDatabases () {
    const mongoClient = (await mongo_client.getMongoClient());
    const pgPool = await pg_client.getPgPool();
    return { mongoClient, pgPool }
}

app.get('/update_dataset', async function (req, res) {

    const { mongoClient, pgPool } = await getDatabases();
    console.log('This Ran');
    
    const users = (await pgPool.query(`SELECT  "users".* FROM "users" WHERE "users"."ext_uid" IS NOT NULL AND "users"."ext_uid" != '' LIMIT 10`)).rows;
    // console.log(user);

    const events = (await pgPool.query(`SELECT  "events".* FROM "events" LIMIT 10`)).rows;

    const rewards = (await pgPool.query(`SELECT  user_offers.*, users.ext_uid, COALESCE(users.user_name, users.display_name) AS user_name FROM "user_offers" INNER JOIN "users" ON "users"."id" = "user_offers"."user_id" ORDER BY user_offers.created_at desc LIMIT 10`)).rows;
    // console.log({rewards});    

    const currencies = (await pgPool.query(`SELECT  "currencies".* FROM "currencies" LIMIT 10`)).rows;
    // console.log({currencies});

    const offer_types = (await pgPool.query(`SELECT  "offer_types".* FROM "offer_types" LIMIT 10`)).rows;
    // console.log({offer_types});
    
    const offers = (await pgPool.query(`SELECT  "offers".* FROM "offers" LIMIT 10`)).rows;
    // console.log({offers});

    const stamps = (await pgPool.query(`SELECT  "stamps".* FROM "stamps" LIMIT 10`)).rows;
    // console.log({stamps});

    const campaigns = (await pgPool.query(`SELECT  "campaigns".* FROM "campaigns" LIMIT 10`)).rows;
    // console.log({campaigns});

    const collections = (await pgPool.query(`SELECT  "collections".* FROM "collections" LIMIT 10`)).rows;
    // console.log({collections});

    const bonanza_users = (await pgPool.query(`SELECT  "bonanza_users".* FROM "bonanza_users" LIMIT 10`)).rows;
    // console.log({bonanza_users});

    const referral_events = (await pgPool.query(`SELECT  events.title AS event_label, events.key AS event_key, referee_id, referrer_id, '-' AS ref_user_id, '-' AS user_id, referral_events.created_at FROM "referral_events" INNER JOIN "events" ON "events"."id" = "referral_events"."event_id" AND "events"."key" NOT IN ('set_user', 'set_new_user') ORDER BY referral_events.created_at desc LIMIT 10`)).rows;
    // console.log({referral_events});

    const users_stamp_campaigns = (await pgPool.query(`SELECT users.id, users.ext_uid, COALESCE(users.user_name, users.display_name) AS user_name, campaigns.title AS campaign_title, campaigns.id as campaign_id, collections.status AS campaign_status, collections.created_at AS started_on, collections.updated_at AS completed_on FROM "users" INNER JOIN "collections" ON "collections"."user_id" = "users"."id" INNER JOIN "campaigns" ON "campaigns"."id" = "collections"."campaign_id" WHERE "users"."ext_uid" IS NOT NULL AND "users"."ext_uid" != '' ORDER BY collections.created_at DESC LIMIT 10`)).rows;
    // console.log({users_stamp_campaigns});

    const redemptions = (await pgPool.query(`SELECT  user_debits.*, users.ext_uid, COALESCE(users.user_name, users.display_name) AS user_name FROM "user_debits" INNER JOIN "users" ON "users"."id" = "user_debits"."user_id" ORDER BY user_debits.created_at desc LIMIT 10`)).rows;
    // console.log({redemptions});

    // console.log({ users, events, currencies, offers, offer_types, stamps, campaigns, bonanza_users, rewards,  referral_events,  users_stamp_campaigns,  redemptions});    
    var base_collection_names = [
        'users',
        'events',
        'currencies',
        'offers',
        'offer_types',
        'stamps',
        'campaigns',
        'bonanza_users'
    ]
    var transoformed_collections = [
        'rewards', 
        'referral_events', 
        'users_stamp_campaigns', 
        'redemptions'
    ]
    
    var activeDB = mongoClient.db('reports_db');

    activeDB.listCollections()
    const coll = await activeDB.listCollections().toArray();
    const collectionNames = coll.map(c => c.name);
    // console.log({collectionNames});

    for (let index = 0; index < base_collection_names.length; index++) {
        const collection_name = base_collection_names[index];
        if (!collectionNames.includes(collection_name)) {
            activeDB.createCollection(collection_name, function(err, res) {
                if (err) throw err;
                console.log("Collection created!");
            });
        }
    }

    for (let index = 0; index < transoformed_collections.length; index++) {
        const collection_name = transoformed_collections[index];
        if (!collectionNames.includes(collection_name)) {
            activeDB.createCollection(collection_name, function(err, res) {
                if (err) throw err;
                console.log("Collection created!");
            });
        }
    }

});


const port = process.env.PORT || 3000;
app.listen(port, () => console.log("Running on port " + port))




// {
//     databasesList = await mongoClient.db().admin().listDatabases();
//     console.log("Databases:");
//     databasesList.databases.forEach(db => console.log(` - ${db.name}`));
//     // console.log(mongoClient.db());
// }