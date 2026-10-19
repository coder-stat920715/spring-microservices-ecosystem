// Read-model schema for MongoDB / DocumentDB (run with: mongosh <uri> sql/mongo-init.js)
const dbName = "orders_read";
const database = db.getSiblingDB(dbName);
database.createCollection("order_views", {
  validator: { $jsonSchema: {
    bsonType: "object",
    required: ["_id", "tenantId", "status", "version"],
    properties: {
      _id: { bsonType: "string" }, customerId: { bsonType: "string" }, tenantId: { bsonType: "string" },
      totalAmount: { bsonType: ["decimal", "double", "string"] }, currency: { bsonType: "string" },
      status: { enum: ["PENDING", "PAID", "CANCELLED", "SHIPPED"] },
      version: { bsonType: ["int", "long"] }
    }
  } }
});
database.order_views.createIndex({ tenantId: 1, _id: 1 });
