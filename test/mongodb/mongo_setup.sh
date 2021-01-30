#!/bin/bash
sleep 10

echo SETUP.sh time now: `date +"%T" `
mongo --host M1:27017 <<EOF
rs.initiate({
      _id: "rs0",
      version: 1,
      members: [
         { _id: 0, host : "localhost:27017" }
      ]
   }
)
EOF
