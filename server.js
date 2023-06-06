require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const Redis = require('redis')

const redisClient = Redis.createClient()

redisClient.connect().catch(err => {
    console.error('Error connecting to Redis:', err);
    process.exit(1); // Terminate the application if the connection fails
});



const app = express();
const port = 3000;

// Connect to MongoDB
mongoose.connect(process.env.DB_URL)
  .then(() => console.log('Connected to MongoDB'))
  .catch(error => console.error('Error connecting to MongoDB:', error));

const certificationModel = new mongoose.Schema({
uid: {
    type: String,
    required: true
},
org: {
    type: String,
    required: true
},
work_location: {
    type: String,
},
certification: {
    type: String,
    required: true,
},
issue_date: {
    type: String,
},
type: {
    type: String,
    required: true
},
key: {
    type: String,
    required: true,
    unique: true
}
});

const certificationSchema = mongoose.model("Certifications", certificationModel, "certificationTest")

const DEFAULT_EXPIRATION = 3600

app.get("/redis/connect", async (req, res) => {
    await redisClient.connect();
    res.json("redis connected")
    
});

//Retrive most popular certifications in the Model
app.get("/certifications/cloud/certification", async (req, res) => {
    
    certificationSchema.aggregate([
    { $group: { _id: "$certification", count: { $sum: 1 } } },
    { $project: { word:"$_id" ,group: "$_id", value: "$count", _id: 0 } }, //modify json fields
    { $sort: { value: -1 } },                                 // sort descending
    //{ $limit: 50 }                                          // choose the number of groups
    ])
    .then((results) => {
        res.json(results);
    })
    .catch((err) => {
        console.log(err);
        res.status(500).send('Error retrieving how many times a parameter repeats');
    });
});


  app.get('/api/data/certifications', async (req, res) => {
    try {
        const result = await redisClient.get('certifications');
    
        if (result !== null) {
          // A cached result exists in Redis, so return it
          return res.json(JSON.parse(result));
        }
    
        // Cache is not available in Redis, fetch the data from the database
        certificationSchema.aggregate([
            { $group: { _id: "$certification", count: { $sum: 1 } } },
            { $project: { word: "$_id", group: "$_id", value: "$count", _id: 0 } },
            { $sort: { value: -1 } },
          ])
          .then((results) => {
            redisClient.setEx('certifications', DEFAULT_EXPIRATION, JSON.stringify(results));
            return res.json(results);
          })
          .catch((err) => {
            console.error(err);
            return res.status(500).send('Error retrieving how many times a parameter repeats');
          });
      } catch (error) {
        console.error('Error retrieving value from Redis:', error);
        return res.status(500).json({ error: 'Internal Server Error' });
      }
  });


app.listen(port, () => console.log("Server listening on port", port));